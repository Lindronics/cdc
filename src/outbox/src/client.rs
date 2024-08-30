use anyhow::Context;
use cdc_framework::db::{self, ReplicationConfig};
use uuid::Uuid;

use crate::model;

#[derive(Clone)]
pub struct OutboxClient {
    db_publisher: cdc_framework::Publisher<model::EventRecord>,
    table: String,
}

impl OutboxClient {
    pub async fn new(
        db_config: &db::DbConfig,
        replication_config: &ReplicationConfig,
    ) -> anyhow::Result<Self> {
        let db_client = db::DbClient::new(db_config).await?;
        crate::setup(&db_client, &replication_config.table).await?;

        let db_publisher = cdc_framework::Publisher::new(db_client).await?;
        Ok(Self {
            db_publisher,
            table: replication_config.table.clone(),
        })
    }

    pub async fn persist_one(&self, item: impl model::Message) -> anyhow::Result<()> {
        let client = self.db_publisher.as_ref().await;
        let record = item.into_record();

        client
            .execute(
                &format!(
                    r#"
                    INSERT INTO {table} (
                        id,
                        agg_id,
                        event_type,
                        data,
                        ttl
                    ) VALUES ($1, $2, $3, $4, $5);
                    "#,
                    table = self.table,
                ),
                &[
                    &record.id,
                    &record.agg_id,
                    &record.event_type,
                    &record.data,
                    &record.ttl,
                ],
            )
            .await?;

        Ok(())
    }

    pub async fn persist(
        &self,
        items: impl IntoIterator<Item = impl model::Message>,
    ) -> anyhow::Result<()> {
        let mut client_mut = self.db_publisher.as_mut().await;

        let transaction = client_mut.transaction().await?;
        for item in items {
            let record = item.into_record();
            transaction
                .execute(
                    &format!(
                        r#"
                        INSERT INTO {table} (
                            id,
                            agg_id,
                            event_type,
                            data,
                            ttl
                        ) VALUES ($1, $2, $3, $4, $5);
                        "#,
                        table = self.table,
                    ),
                    &[
                        &record.id,
                        &record.agg_id,
                        &record.event_type,
                        &record.data,
                        &record.ttl,
                    ],
                )
                .await?;
        }
        transaction.commit().await?;

        Ok(())
    }

    pub async fn get_dead_messages(&self) -> anyhow::Result<Vec<Uuid>> {
        let client = self.db_publisher.as_ref().await;
        let rows = client
            .query("SELECT * FROM $1 WHERE ttl <= 0", &[&self.table])
            .await?;

        rows.into_iter()
            .map(|row| row.try_get("id"))
            .collect::<Result<Vec<_>, _>>()
            .context("Error getting dead messages")
    }

    pub(crate) async fn update_ttl(&self, id: Uuid, ttl: i16) -> anyhow::Result<()> {
        let client = self.db_publisher.as_ref().await;

        client
            .execute(
                &format!("UPDATE {} SET ttl = $2 WHERE id = $1;", self.table),
                &[&id, &(ttl - 1)],
            )
            .await
            .context("Error updating TTL")
            .map(|_| ())?;

        Ok(())
    }
}
