use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use subscriber::{message::PrintHandler, stream::Subscriber};
use tokio_postgres::{types::PgLsn, NoTls};
use uuid::Uuid;

mod publisher;
mod subscriber;

pub async fn asdf() {
    let (client, connection) = tokio_postgres::connect(
        "user=postgres password=password host=localhost port=5432 dbname=postgres replication=database",
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(connection);

    client
        .simple_query(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                id UUID PRIMARY KEY,
                agg_id UUID NOT NULL,
                data BYTEA NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            "#,
        )
        .await
        .unwrap();

    // client
    //     .simple_query(
    //         r#"
    //         CREATE PUBLICATION events_pub
    //         FOR TABLE events
    //         WITH (publish = 'insert');
    //         "#,
    //     )
    //     .await
    //     .unwrap();
    // client
    //     .simple_query("CREATE_REPLICATION_SLOT events_slot LOGICAL \"pgoutput\" NOEXPORT_SNAPSHOT")
    //     .await
    //     .unwrap();

    let _bg = tokio::spawn(handle_stream());

    insert_some_records().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

async fn handle_stream() {
    let mut handler = Subscriber::new(PrintHandler).await.unwrap();
    handler.handle_stream().await.unwrap();
}

async fn insert_some_records() {
    let (client, connection) = tokio_postgres::connect(
        "user=postgres password=password host=localhost port=5432 dbname=postgres",
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(connection);

    for i in 0..10 {
        let bin_data = &[i as u8];
        client
            .query(
                "INSERT INTO events (id, agg_id, data) VALUES ($1, $2, $3)",
                &[&Uuid::new_v4(), &Uuid::new_v4(), &bin_data.as_slice()],
            )
            .await
            .unwrap();
    }
}
