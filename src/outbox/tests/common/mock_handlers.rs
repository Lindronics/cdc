use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use outbox::{model::EventRecord, EventHandler};

pub struct FallibleHandler<Inner: EventHandler<EventRecord>> {
    pub succeed_on: usize,
    pub attempts: Arc<AtomicU32>,
    pub inner: Inner,
}

impl<Inner: EventHandler<EventRecord> + Send + Sync> EventHandler<EventRecord>
    for FallibleHandler<Inner>
{
    async fn handle(&self, msg: EventRecord) -> anyhow::Result<()> {
        let prev = self.attempts.fetch_add(1, Ordering::Relaxed);

        if msg.ttl as usize <= self.succeed_on {
            self.inner.handle(msg).await
        } else {
            anyhow::bail!("Failed on attempt {}", prev);
        }
    }
}
