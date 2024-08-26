use cdc_framework::db;

use crate::model;

const INSERT_SQL: &str = r#"
    INSERT INTO events (
        id,
        agg_id,
        event_type,
        data
    ) VALUES ($1, $2, $3, $4);
"#;

pub struct OutboxClient {
    db_publisher: cdc_framework::Publisher<model::EventRecord>,
}

impl OutboxClient {
    pub async fn new(db_config: &db::DbConfig) -> anyhow::Result<Self> {
        let db_client = db::DbClient::new(db_config).await?;
        crate::setup(&db_client).await?;

        let db_publisher = cdc_framework::Publisher::new(db_client).await?;
        Ok(Self { db_publisher })
    }

    pub async fn persist_one(&self, item: impl model::Event) -> anyhow::Result<()> {
        let client = self.db_publisher.as_ref().await;
        let record = item.into_record();

        client
            .execute(
                INSERT_SQL,
                &[&record.id, &record.agg_id, &record.event_type, &record.data],
            )
            .await?;

        Ok(())
    }

    pub async fn persist(
        &self,
        items: impl IntoIterator<Item = impl model::Event>,
    ) -> anyhow::Result<()> {
        let mut client_mut = self.db_publisher.as_mut().await;

        let transaction = client_mut.transaction().await?;
        for item in items {
            let record = item.into_record();
            transaction
                .execute(
                    INSERT_SQL,
                    &[&record.id, &record.agg_id, &record.event_type, &record.data],
                )
                .await?;
        }
        transaction.commit().await?;

        Ok(())
    }
}
