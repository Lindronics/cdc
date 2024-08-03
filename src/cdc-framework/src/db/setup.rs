pub async fn setup_db(client: &tokio_postgres::Client) -> anyhow::Result<()> {
    // Setup aggregate store
    client
        .simple_query(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id UUID PRIMARY KEY,
                agg_id UUID NOT NULL,
                event_type TEXT NOT NULL,
                data BYTEA NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#,
        )
        .await?;

    // Setup publication if not exists
    if !publication_exists(client).await? {
        client
            .simple_query(
                r#"
                CREATE PUBLICATION events_pub
                FOR TABLE events
                WITH (publish = 'insert');
                "#,
            )
            .await?;
    }

    // Setup replication slot if not exists
    if !replication_slot_exists(client).await? {
        client
            .simple_query(
                r#"
                CREATE_REPLICATION_SLOT events_slot 
                LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT
                "#,
            )
            .await?;
    }

    Ok(())
}

pub async fn publication_exists(client: &tokio_postgres::Client) -> anyhow::Result<bool> {
    let publications = client
        .simple_query("SELECT * FROM pg_publication WHERE pubname = 'events_pub'")
        .await?;
    Ok(publications
        .into_iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .is_some())
}

pub async fn replication_slot_exists(client: &tokio_postgres::Client) -> anyhow::Result<bool> {
    let publications = client
        .simple_query("SELECT * FROM pg_replication_slots WHERE slot_name = 'events_slot'")
        .await?;
    Ok(publications
        .into_iter()
        .find_map(|msg| match msg {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .is_some())
}
