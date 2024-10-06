use cdc_framework::{
    db::{DbClient, DbConfig, ReplicationConfig},
    EventHandler,
};
use tokio::sync::RwLock;

use crate::{model::EventRecord, setup};

pub struct OutboxSubscriber<H>
where
    H: EventHandler<EventRecord>,
{
    inner: RwLock<cdc_framework::Subscriber<EventRecord, H>>,
}

impl<H> OutboxSubscriber<H>
where
    H: EventHandler<EventRecord> + Send + Sync + 'static,
{
    pub async fn new(
        db_config: &DbConfig,
        replication_config: &ReplicationConfig,
        handler: H,
    ) -> anyhow::Result<Self> {
        let replication_client = DbClient::<true>::new(db_config).await?;
        setup(&replication_client, &replication_config.table).await?;

        let inner = RwLock::new(
            cdc_framework::Subscriber::new(&replication_client, replication_config, handler)
                .await?,
        );

        Ok(Self { inner })
    }

    pub async fn listen(&self) -> anyhow::Result<()> {
        self.inner.write().await.listen().await
    }
}
