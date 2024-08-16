use super::model::Entity;

impl<const REPLICATION: bool> super::DbClient<REPLICATION> {
    pub async fn setup<T: Entity>(&self) -> anyhow::Result<()> {
        // Table has to exist
        anyhow::ensure!(self.table_exists(T::TABLE).await?);

        // Setup publication if not exists
        if !self.publication_exists(T::TABLE).await? {
            self.simple_query(&format!(
                r#"
                CREATE PUBLICATION {table}_pub
                FOR TABLE {table}
                WITH (publish = 'insert');
                "#,
                table = T::TABLE
            ))
            .await?;
        }

        // Setup replication slot if not exists
        if !self.replication_slot_exists(T::TABLE).await? {
            self.simple_query(&format!(
                r#"
                CREATE_REPLICATION_SLOT {table}_slot 
                LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT
                "#,
                table = T::TABLE
            ))
            .await?;
        }

        Ok(())
    }

    async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
        let tables = self
            .simple_query(&format!(
                "SELECT * FROM pg_catalog.pg_tables WHERE pg_tables.tablename = '{table}'"
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

    async fn publication_exists(&self, table: &str) -> anyhow::Result<bool> {
        let publications = self
            .simple_query(&format!(
                "SELECT * FROM pg_publication WHERE pubname = '{table}_pub'"
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

    async fn replication_slot_exists(&self, table: &str) -> anyhow::Result<bool> {
        let publications = self
            .simple_query(&format!(
                "SELECT * FROM pg_replication_slots WHERE slot_name = '{table}_slot'"
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
