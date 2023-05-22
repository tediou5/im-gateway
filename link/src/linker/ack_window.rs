struct Ack {
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

#[derive(Debug, Clone)]
pub(super) struct AckWindow<T: std::hash::Hash + std::cmp::Ord + std::cmp::PartialOrd> {
    semaphore: std::rc::Rc<local_sync::semaphore::Semaphore>,
    ack_list: std::rc::Rc<std::cell::RefCell<std::collections::BTreeMap<T, Ack>>>,
}

impl<T> AckWindow<T>
where
    T: std::hash::Hash + std::cmp::Ord + std::cmp::PartialOrd,
{
    pub(super) fn new(permits: u8) -> Self {
        let semaphore = local_sync::semaphore::Semaphore::new(permits.into());
        let semaphore = semaphore.into();

        let ack_list = std::cell::RefCell::new(std::collections::BTreeMap::new());
        let ack_list = ack_list.into();

        Self {
            semaphore,
            ack_list,
        }
    }

    pub(super) fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub(super) async fn acquire(
        &self,
        trace_id: T,
        message: std::rc::Rc<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let permit = (self.semaphore.clone()).acquire_owned().await?;
        let ack = Ack { permit, message };
        self.ack_list.borrow_mut().insert(trace_id, ack);
        Ok(())
    }

    pub(super) async fn ack(&self, trace_id: T) -> anyhow::Result<()> {
        self.ack_list.borrow_mut().remove(&trace_id);
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::println;

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
                ack_list.ack(1).await.unwrap();
                println!("ack window: ack trace: 1");
            });

            ack_c.acquire(2, vec![0].into()).await.unwrap();
            println!("ack window: acquire trace: 2")
        });
        rt.block_on(local_set);
    }
}
