#[derive(Debug, PartialEq)]
pub(super) struct Retry {
    pub(super) times: usize,
    pub(super) messages: Vec<std::rc::Rc<Vec<u8>>>,
}

pub(super) struct Ack {
    permit: local_sync::semaphore::OwnedSemaphorePermit,
    message: std::rc::Rc<Vec<u8>>,
    skip: bool,
}

impl std::fmt::Debug for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ack")
            .field("skip", &self.skip)
            .field("permit", &self.permit)
            .field("message", &self.message)
            .finish()
    }
}

pub(crate) struct AckWindow {
    retry_times: std::rc::Rc<std::cell::Cell<usize>>,
    semaphore: std::rc::Rc<local_sync::semaphore::Semaphore>,
    ack_list: std::rc::Rc<std::cell::RefCell<std::collections::BTreeMap<u64, Ack>>>,
    waker: std::rc::Rc<std::cell::Cell<Option<std::task::Waker>>>,
}

impl std::fmt::Debug for AckWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckWindow")
            .field("retry_times", &self.retry_times)
            .field(
                "semaphore available permits",
                &self.semaphore.available_permits(),
            )
            .finish()
    }
}

impl Clone for AckWindow {
    fn clone(&self) -> Self {
        Self {
            retry_times: self.retry_times.clone(),
            semaphore: self.semaphore.clone(),
            ack_list: self.ack_list.clone(),
            waker: self.waker.clone(),
        }
    }
}

impl AckWindow {
    pub(super) fn new(permits: usize) -> Self {
        let semaphore = local_sync::semaphore::Semaphore::new(permits);
        let semaphore = semaphore.into();

        let ack_list = std::cell::RefCell::new(std::collections::BTreeMap::new());
        let ack_list = ack_list.into();

        Self {
            retry_times: std::cell::Cell::new(0).into(),
            semaphore,
            ack_list,
            waker: std::cell::Cell::new(None).into(),
        }
    }

    pub(super) fn run(
        self,
        pin: &str,
        mut close_notify: tokio::sync::broadcast::Receiver<()>,
        tcp_collect: local_sync::mpsc::unbounded::Tx<crate::linker::SenderEvent>,
        event_rx: local_sync::stream_wrappers::unbounded::ReceiverStream<crate::linker::Event>,
    ) {
        self.acquire(pin, tcp_collect.clone(), event_rx);

        let retry_config = &crate::RETRY_CONFIG.get().unwrap();

        let crate::config::Retry {
            timeout, max_times, ..
        } = retry_config;

        let pin = pin.to_string();
        tokio::task::spawn_local(async move {
            'retry: loop {
                let retry = tokio::select! {
                    retry = self._try_again() => retry,
                    _ = close_notify.recv() => break,
                };

                let retry_timeout = match super::ack_window::AckWindow::get_retry_timeout(
                    retry.times,
                    *timeout,
                    *max_times,
                ) {
                    Ok(timeout) => timeout,
                    Err(_) => break 'retry,
                };
                for message in retry.messages.iter() {
                    if let Err(e) =
                        tcp_collect.send(crate::linker::SenderEvent::WriteBatch(message.clone()))
                    {
                        tracing::error!("tcp error: send retry message error: {e:?}");
                        break 'retry;
                    };
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(retry_timeout)).await;
            }
            tracing::error!("[{pin}]retry error, close connection");
            let _ = tcp_collect.send(crate::linker::SenderEvent::Close);
        });
    }

    fn acquire(
        &self,
        pin: &str,
        tcp_collect: local_sync::mpsc::unbounded::Tx<crate::linker::SenderEvent>,
        mut event_rx: local_sync::stream_wrappers::unbounded::ReceiverStream<crate::linker::Event>,
    ) {
        let pin = pin.to_string();
        let ack_window = self.clone();
        tokio::task::spawn_local(async move {
            use tokio_stream::StreamExt as _;

            while let Some(event) = event_rx.next().await {
                let (trace_id, message) = match event {
                    crate::linker::Event::WriteBatch(trace_id, message) => (trace_id, message),
                    crate::linker::Event::Close => {
                        let _ = tcp_collect.send(crate::linker::SenderEvent::Close);
                        return;
                    }
                };

                if let Err(e) = ack_window._acquire(pin.as_str(), trace_id, &message).await {
                    tracing::error!("acquire ack windows failed: {e}");
                    let _ = tcp_collect.send(crate::linker::SenderEvent::Close);
                    return;
                }

                let _ = tcp_collect.send(crate::linker::SenderEvent::WriteBatch(message));
            }
        });
    }

    async fn _acquire(
        &self,
        pin: &str,
        trace_id: u64,
        message: &std::rc::Rc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let permit = (self.semaphore.clone()).acquire_owned().await?;
        let ack = Ack {
            permit,
            message: message.clone(),
            skip: true,
        };
        tracing::debug!("[{pin}]AckWindow: acquire trace_id: {trace_id:?}");
        let mut ack_list = self.ack_list.borrow_mut();
        let flag = ack_list.len();
        // if acquire a new ack & waker is set & never retry before, wake it.
        if let None = ack_list.insert(trace_id, ack) &&
        let Some(w) = self.waker.replace(None) &&
        let 0 = flag {
            tracing::trace!("[{pin}]AckWindow: acquire: wake future...");
            w.wake();
        };
        Ok(())
    }

    pub(super) fn ack(&self, pin: &str, trace_id: u64) {
        if self.ack_list.borrow_mut().remove(&trace_id).is_some() {
            tracing::debug!("[{pin}]AckWindow: ack trace_id: {trace_id:?}");
            self.retry_times.replace(0);
        };
    }

    async fn _try_again(&self) -> Retry {
        futures::future::poll_fn(|cx| self.poll_try_again(cx)).await
    }

    fn poll_try_again(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Retry> {
        let mut ack_list = self.ack_list.borrow_mut();
        if !ack_list.is_empty() {
            let time = self.retry_times.get() + 1;
            let times = self.retry_times.replace(time);
            let messages = ack_list
                .iter_mut()
                .filter_map(|(_, Ack { message, skip, .. })| {
                    if *skip {
                        *skip = false;
                        None
                    } else {
                        Some(message.clone())
                    }
                })
                .collect();
            return std::task::Poll::Ready(Retry { times, messages });
        }
        self.waker.replace(Some(cx.waker().clone()));
        std::task::Poll::Pending
    }

    fn get_retry_timeout(retry_times: usize, timeout: usize, max: usize) -> anyhow::Result<u64> {
        let times: u64 = match retry_times {
            less_than_three if less_than_three < 3 => less_than_three as u64 + 1,
            other if other < max => 4,
            _ => {
                tracing::error!("retry to many times, close connection");
                return Err(anyhow::anyhow!("retry to many times, close connection"));
            }
        };
        Ok(times * timeout as u64)
    }
}

#[cfg(test)]
mod test {
    use super::{AckWindow, Retry};

    #[test]
    fn wait_for_acquire() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let local = tokio::task::LocalSet::new();
        let local_set = local.run_until(async {
            let ack_list = super::AckWindow::new(1);
            ack_list._acquire("test", 1, &vec![0].into()).await.unwrap();
            let ack_c = ack_list.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                ack_list.ack("test", 1);
                println!("ack window: ack trace: 1");
            });

            ack_c._acquire("test", 2, &vec![0].into()).await.unwrap();
            println!("ack window: acquire trace: 2")
        });
        rt.block_on(local_set);
    }

    #[test]
    fn wait_for_retry_and_reset() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let local = tokio::task::LocalSet::new();
        let local_set = local.run_until(async {
            let ack_list = super::AckWindow::new(1);
            let ack_c = ack_list.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                ack_c._acquire("test", 1, &vec![0].into()).await.unwrap();
                println!("ack window: ack trace: 1");
            });

            let retry1 = ack_list._try_again().await;
            println!("ack window: retry 1: {retry1:?}");
            assert_eq!(
                retry1,
                Retry {
                    times: 0,
                    messages: vec![]
                }
            );

            let retry2 = ack_list._try_again().await;
            println!("ack window: retry 2: {retry2:?}");
            assert_eq!(
                retry2,
                Retry {
                    times: 1,
                    messages: vec![vec![0].into()]
                }
            );

            let ack_c = ack_list.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                ack_c._acquire("test", 2, &vec![0].into()).await.unwrap();
                println!("ack window: ack trace: 2");
            });

            let retry3 = ack_list._try_again().await;
            println!("ack window: retry 3: {retry3:?}");
            assert_eq!(
                retry3,
                Retry {
                    times: 2,
                    messages: vec![vec![0].into()]
                }
            );
        });
        rt.block_on(local_set);
    }

    #[test]
    fn get_retry_timeout() {
        let timeout = AckWindow::get_retry_timeout(0, 3, 10).unwrap();
        assert_eq!(timeout, 3);
        let timeout = AckWindow::get_retry_timeout(1, 3, 10).unwrap();
        assert_eq!(timeout, 6);
        let timeout = AckWindow::get_retry_timeout(4, 3, 10).unwrap();
        assert_eq!(timeout, 12);
        let timeout = AckWindow::get_retry_timeout(8, 3, 10).unwrap();
        assert_eq!(timeout, 12);
        let timeout = AckWindow::get_retry_timeout(10, 3, 10);
        assert!(timeout.is_err());
        let timeout = AckWindow::get_retry_timeout(13, 3, 10);
        assert!(timeout.is_err());
    }
}
