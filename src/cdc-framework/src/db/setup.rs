use super::config::ReplicationConfig;

impl<const REPLICATION: bool> super::DbClient<REPLICATION> {
    pub async fn setup(&self, config: &ReplicationConfig) -> anyhow::Result<()> {
        // Table has to exist
        anyhow::ensure!(self.table_exists(&config.table).await?);

        // Setup publication if not exists
        if !self.publication_exists(&config.publication).await? {
            self.simple_query(&format!(
                r#"
                CREATE PUBLICATION {publication}
                FOR TABLE "{table}"
                WITH (publish = 'insert, update');
                "#,
                table = config.table,
                publication = config.publication,
            ))
            .await?;
        }

        // Setup replication slot if not exists
        if !self
            .replication_slot_exists(&config.replication_slot)
            .await?
        {
            self.simple_query(&format!(
                r#"
                CREATE_REPLICATION_SLOT "{slot}"
                LOGICAL "pgoutput" NOEXPORT_SNAPSHOT;
                "#,
                slot = config.replication_slot,
            ))
            .await?;
        }

        Ok(())
    }

    async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
        let tables = self
            .simple_query(&format!(
                "SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.tablename = '{table}';"
            ))
            .await?;
        Ok(tables
            .into_iter()
            .find_map(|msg| match msg {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .is_some())
    }

    async fn publication_exists(&self, publication: &str) -> anyhow::Result<bool> {
        let publications = self
            .simple_query(&format!(
                "SELECT * FROM pg_publication WHERE pubname = '{publication}';"
            ))
            .await?;
        Ok(publications
            .into_iter()
            .find_map(|msg| match msg {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .is_some())
    }

    async fn replication_slot_exists(&self, slot: &str) -> anyhow::Result<bool> {
        let publications = self
            .simple_query(&format!(
                r#"
                SELECT *
                FROM pg_replication_slots
                WHERE slot_name = '{slot}'
                AND database = '{db}';
                "#,
                db = self.dbname,
                slot = slot,
            ))
            .await?;
        Ok(publications
            .into_iter()
            .find_map(|msg| match msg {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .is_some())
    }
}
