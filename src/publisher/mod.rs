use anyhow::Context;
use tokio::sync::RwLock;
use tokio_postgres::NoTls;

use crate::{db, model::MessageRecord};

pub struct Publisher {
    client: RwLock<tokio_postgres::Client>,
}

impl Publisher {
    pub async fn new() -> anyhow::Result<Self> {
        let (client, connection) = tokio_postgres::connect(
            "user=postgres password=password host=localhost port=5432 dbname=postgres",
            NoTls,
        )
        .await
        .unwrap();
        tokio::spawn(connection);

        // Ensure the database is setup
        db::setup_db(&client).await?;

        Ok(Self {
            client: RwLock::new(client),
        })
    }

    pub async fn persist_one(&self, event: MessageRecord) -> anyhow::Result<()> {
        let client_mut = self
            .client
            .try_read()
            .context("failed to acquire read lock")?;

        client_mut
            .execute(
                r#"
                INSERT INTO events (
                    id,
                    agg_id,
                    event_type,
                    data
                ) VALUES ($1, $2, $3, $4)
                "#,
                &[
                    &event.id,
                    &event.agg_id,
                    &event.event_type,
                    &event.data.as_bytes(),
                ],
            )
            .await?;

        Ok(())
    }
    pub async fn persist(
        &self,
        events: impl IntoIterator<Item = MessageRecord>,
    ) -> anyhow::Result<()> {
        let mut client_mut = self
            .client
            .try_write()
            .context("failed to acquire write lock")?;

        let transaction = client_mut.transaction().await?;
        for event in events {
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
                    &[
                        &event.id,
                        &event.agg_id,
                        &event.event_type,
                        &event.data.as_bytes(),
                    ],
                )
                .await?;
        }
        transaction.commit().await?;

        Ok(())
    }
}
