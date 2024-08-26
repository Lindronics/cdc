use cdc_framework::{db::Entity, Subscriber};
pub use cdc_framework::{
    db::{DbClient, DbConfig},
    InsertHandler as EventHandler,
};
use model::EventRecord;

pub mod amqp;
pub mod client;
pub mod model;

pub async fn new<T>(
    db_config: &DbConfig,
    amqp_connection: &::amqp::Connection,
) -> anyhow::Result<(
    client::OutboxClient,
    Subscriber<EventRecord, amqp::AmqpPublisher<T>>,
)>
where
    T: model::Event + ::amqp::Message + Send + Sync + 'static,
{
    let replication_client = DbClient::<true>::new(db_config).await?;
    let amqp_publisher = ::amqp::AmqpPublisher::new(amqp_connection).await?;

    let sub = Subscriber::new(
        &replication_client,
        amqp::AmqpPublisher::new(amqp_publisher),
    )
    .await?;
    let client = client::OutboxClient::new(db_config).await?;
    Ok((client, sub))
}

pub async fn setup(client: &DbClient<false>) -> anyhow::Result<()> {
    client
        .simple_query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table} (
                id UUID PRIMARY KEY,
                agg_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                data BYTEA NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#,
            table = model::EventRecord::TABLE
        ))
        .await?;

    Ok(())
}
