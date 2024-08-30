use crate::{client::OutboxClient, model::EventRecord};

/// Retries handling of messages that failed to be processed.
///
/// This is done by updating the TTL of the message,
/// which will result in it reappearing in the stream.
///
/// We call this eager since there is no scheduled timeout before the next retry.
///
/// Wraps an inner handler.
pub struct EagerRetryHandler<Inner: cdc_framework::EventHandler<EventRecord>> {
    client: OutboxClient,
    inner: Inner,
}

impl<Inner> EagerRetryHandler<Inner>
where
    Inner: cdc_framework::EventHandler<EventRecord>,
{
    pub async fn new(client: OutboxClient, inner: Inner) -> anyhow::Result<Self> {
        Ok(Self { client, inner })
    }
}

impl<Inner> cdc_framework::EventHandler<EventRecord> for EagerRetryHandler<Inner>
where
    Inner: cdc_framework::EventHandler<EventRecord> + Send + Sync,
{
    async fn handle(&self, msg: EventRecord) -> anyhow::Result<()> {
        let id = msg.id;
        let ttl = msg.ttl;

        if ttl <= 0 {
            println!("TTL expired: {:?}", msg.id);
            return Ok(());
        }

        println!("Handling msg: {id} (TTL {ttl})");
        if let Err(e) = self.inner.handle(msg).await {
            println!("Retrying: {:?}", e);
            self.client.update_ttl(id, ttl).await?;
        }

        Ok(())
    }
}
