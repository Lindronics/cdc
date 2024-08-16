use anyhow::Context;
use tokio::sync::RwLock;

use crate::db::{self, EventRecord};

pub struct Publisher {
    client: RwLock<db::DbClient>,
}

impl Publisher {
    pub async fn new(db_client: db::DbClient) -> anyhow::Result<Self> {
        Ok(Self {
            client: RwLock::new(db_client),
        })
    }

    pub async fn persist_one<T>(&self, event: T) -> anyhow::Result<()>
    where
        EventRecord: From<T>,
    {
        let event = EventRecord::from(event);

        let client = self
            .client
            .try_read()
            .context("failed to acquire read lock")?;

        client
            .execute(
                r#"
                INSERT INTO events (
                    id,
                    agg_id,
                    event_type,
                    data
                ) VALUES ($1, $2, $3, $4)
                "#,
                &[&event.id, &event.agg_id, &event.event_type, &event.data],
            )
            .await?;

        Ok(())
    }
    pub async fn persist<T>(&self, events: impl IntoIterator<Item = T>) -> anyhow::Result<()>
    where
        EventRecord: From<T>,
    {
        let mut client_mut = self
            .client
            .try_write()
            .context("failed to acquire write lock")?;

        let transaction = client_mut.transaction().await?;
        for event in events {
            let event = EventRecord::from(event);
            transaction
                .execute(
                    r#"
                    INSERT INTO events (
                        id,
                        agg_id,
                        event_type,
                        data
                    ) VALUES ($1, $2, $3, $4)
                    "#,
                    &[&event.id, &event.agg_id, &event.event_type, &event.data],
                )
                .await?;
        }
        transaction.commit().await?;

        Ok(())
    }
}
