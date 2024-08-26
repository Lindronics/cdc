use cdc_framework::db::DbConfig;

use crate::{client::OutboxClient, model::EventRecord};

pub struct RetryHandler<Inner: cdc_framework::InsertHandler<EventRecord>> {
    client: OutboxClient,
    inner: Inner,
}

impl<Inner> RetryHandler<Inner>
where
    Inner: cdc_framework::InsertHandler<EventRecord>,
{
    pub async fn new(db_config: &DbConfig, inner: Inner) -> anyhow::Result<Self> {
        Ok(Self {
            client: OutboxClient::new(db_config).await?,
            inner,
        })
    }
}

impl<Inner> cdc_framework::InsertHandler<EventRecord> for RetryHandler<Inner>
where
    Inner: cdc_framework::InsertHandler<EventRecord> + Send + Sync,
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
