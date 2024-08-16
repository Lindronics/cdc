use cdc_framework::db::Entity;
pub use cdc_framework::{
    db::{DbClient, DbConfig},
    InsertHandler as EventHandler,
};

pub mod model;

pub type Publisher = cdc_framework::Publisher<model::EventRecord>;
pub type Subscriber<Handler> = cdc_framework::Subscriber<model::EventRecord, Handler>;

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
