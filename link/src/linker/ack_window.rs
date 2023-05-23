#[derive(Debug, PartialEq)]
pub(super) struct Retry {
    pub(super) times: u8,
    pub(super) messages: Vec<std::rc::Rc<Vec<u8>>>,
}

pub(super) struct Ack {
    permit: local_sync::semaphore::OwnedSemaphorePermit,
    message: std::rc::Rc<Vec<u8>>,
}

impl std::fmt::Debug for Ack {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ack")
            .field("permit", &self.permit)
            .field("message", &self.message)
            .finish()
    }
}

pub(super) struct AckWindow<T: std::hash::Hash + std::cmp::Ord + std::cmp::PartialOrd + Clone> {
    retry_times: std::rc::Rc<std::sync::atomic::AtomicU8>,
    semaphore: std::rc::Rc<local_sync::semaphore::Semaphore>,
    ack_list: std::rc::Rc<std::cell::RefCell<std::collections::BTreeMap<T, Ack>>>,
    waker: std::rc::Rc<std::cell::Cell<Option<std::task::Waker>>>,
}

impl<T> Clone for AckWindow<T>
where
    T: std::hash::Hash + std::cmp::Ord + std::cmp::PartialOrd + Clone,
{
    fn clone(&self) -> Self {
        Self {
            retry_times: self.retry_times.clone(),
            semaphore: self.semaphore.clone(),
            ack_list: self.ack_list.clone(),
            waker: self.waker.clone(),
        }
    }
}

impl<T> AckWindow<T>
where
    T: std::hash::Hash + std::cmp::Ord + std::cmp::PartialOrd + Clone,
{
    pub(super) fn new(permits: u8) -> Self {
        let semaphore = local_sync::semaphore::Semaphore::new(permits.into());
        let semaphore = semaphore.into();

        let ack_list = std::cell::RefCell::new(std::collections::BTreeMap::new());
        let ack_list = ack_list.into();

        Self {
            retry_times: std::sync::atomic::AtomicU8::new(0).into(),
            semaphore,
            ack_list,
            waker: std::cell::Cell::new(None).into(),
        }
    }

    pub(super) fn _available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub(super) async fn acquire(
        &self,
        trace_id: T,
        message: std::rc::Rc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let permit = (self.semaphore.clone()).acquire_owned().await?;
        let ack = Ack { permit, message };

        // if acquire a new ack & waker is set & never retry before, wake it.
        if let None = self.ack_list.borrow_mut().insert(trace_id, ack) &&
        let Some(w) = self.waker.replace(None) {
            w.wake();
        };
        Ok(())
    }

    pub(super) fn ack(&self, trace_id: T) -> anyhow::Result<()> {
        if self.ack_list.borrow_mut().remove(&trace_id).is_some() {
            self.retry_times
                .swap(0, std::sync::atomic::Ordering::SeqCst);
        };
        Ok(())
    }

    pub(super) async fn try_again(&self) -> Retry {
        futures::future::poll_fn(|cx| self.poll_try_again(cx)).await
    }

    pub(super) fn poll_try_again(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Retry> {
        let ack_list = self.ack_list.borrow();
        if !ack_list.is_empty() {
            let times = self
                .retry_times
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let messages = ack_list
                .iter()
                .map(|(_, ack)| ack.message.clone())
                .collect();
            return std::task::Poll::Ready(Retry { times, messages });
        }
        self.waker.replace(Some(cx.waker().clone()));
        std::task::Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use super::Retry;

    #[test]
    fn wait_for_acquire() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let local = tokio::task::LocalSet::new();
        let local_set = local.run_until(async {
            let ack_list = super::AckWindow::new(1);
            ack_list.acquire(1, vec![0].into()).await.unwrap();
            let ack_c = ack_list.clone();
            tokio::task::spawn_local(async move {
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                ack_list.ack(1).unwrap();
                println!("ack window: ack trace: 1");
            });

            ack_c.acquire(2, vec![0].into()).await.unwrap();
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
                ack_c.acquire(1, vec![0].into()).await.unwrap();
                println!("ack window: ack trace: 1");
            });

            let retry1 = ack_list.try_again().await;
            println!("ack window: retry 1: {retry1:?}");
            assert_eq!(
                retry1,
                Retry {
                    times: 0,
                    messages: vec![vec![0].into()]
                }
            );

            let retry2 = ack_list.try_again().await;
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
                ack_c.acquire(2, vec![0].into()).await.unwrap();
                println!("ack window: ack trace: 2");
            });

            ack_list.ack(1).unwrap();
            let retry3 = ack_list.try_again().await;
            println!("ack window: retry 3: {retry3:?}");
            assert_eq!(
                retry3,
                Retry {
                    times: 0,
                    messages: vec![vec![0].into()]
                }
            );
        });
        rt.block_on(local_set);
    }
}
