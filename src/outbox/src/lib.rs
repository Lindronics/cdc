pub use cdc_framework::{
    db::{DbClient, DbConfig, ReplicationConfig},
    EventHandler,
};

pub mod client;
pub mod handlers;
pub mod model;
pub mod subscriber;

pub async fn setup<const REPLICATION: bool>(
    client: &DbClient<REPLICATION>,
    table: &str,
) -> anyhow::Result<()> {
    client
        .simple_query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS "{table}" (
                id UUID PRIMARY KEY,
                agg_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                data BYTEA NOT NULL,
                ttl smallint NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#
        ))
        .await?;

    Ok(())
}
