use core::panic;
use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, Tuple, TupleData,
};
use tokio_postgres::{
    types::{FromSql, PgLsn},
    NoTls, SimpleQueryMessage,
};
use uuid::Uuid;

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

    let result = client
        .simple_query(
            "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'events_slot'",
        )
        .await
        .unwrap();
    let rows = result
        .into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect::<Vec<_>>();
    let first = rows.first().unwrap();
    let lsn: PgLsn = first
        .get("confirmed_flush_lsn")
        .unwrap()
        .to_string()
        .parse()
        .unwrap();
    dbg!(lsn);

    let options = [("proto_version", "1"), ("publication_names", "events_pub")];
    let query = format!(
        "START_REPLICATION SLOT events_slot LOGICAL {lsn} ({});",
        options
            .iter()
            .map(|(k, v)| format!("\"{}\" '{}'", k, v))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let duplex_stream = Box::pin(
        client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap(),
    );
    let _bg = tokio::spawn(handle_stream(duplex_stream, lsn));

    // insert_some_records().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

async fn handle_stream(stream: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>, lsn: PgLsn) {
    let mut handler = StreamHandler {
        stream,
        lsn,
        handler: PrintHandler,
    };
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

struct StreamHandler<T: MessageHandler> {
    stream: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>,
    lsn: PgLsn,
    handler: T,
}

impl<T: MessageHandler> StreamHandler<T> {
    async fn handle_stream(&mut self) -> anyhow::Result<()> {
        while let Some(msg) = self.stream.as_mut().next().await {
            let msg = msg.context("could not get next message in stream")?;

            let ReplicationMessage::XLogData(data) = ReplicationMessage::parse(&msg)? else {
                continue;
            };

            let LogicalReplicationMessage::Insert(msg) =
                LogicalReplicationMessage::parse(data.data())?
            else {
                continue;
            };

            let record = MessageRecord::try_from(msg.tuple())?;
            self.handler.handle(record).await?;

            let ssu = prepare_ssu(self.lsn);
            self.stream.as_mut().send(ssu).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
struct MessageRecord {
    id: Uuid,
    agg_id: Uuid,
    data: String,
}

impl TryFrom<&Tuple> for MessageRecord {
    type Error = anyhow::Error;

    fn try_from(value: &Tuple) -> Result<Self, Self::Error> {
        fn raw_text(data: &TupleData) -> anyhow::Result<Cow<str>> {
            match data {
                TupleData::Text(x) => Ok(String::from_utf8_lossy(x)),
                _ => anyhow::bail!("expected text"),
            }
        }

        let row = value.tuple_data();

        let id = {
            let x = raw_text(row.first().context("missing col 0")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let agg_id = {
            let x = raw_text(row.get(1).context("missing col 1")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let data = raw_text(row.get(2).context("missing col 2")?)?.into();
        Ok(Self { id, agg_id, data })
    }
}

trait MessageHandler {
    fn handle(&self, msg: MessageRecord) -> impl Future<Output = anyhow::Result<()>> + Send;
}

struct PrintHandler;

impl MessageHandler for PrintHandler {
    async fn handle(&self, msg: MessageRecord) -> anyhow::Result<()> {
        println!("{:?}", msg);
        Ok(())
    }
}

fn prepare_ssu(write_lsn: PgLsn) -> Bytes {
    const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;

    let write_lsn_bytes = u64::from(write_lsn).to_be_bytes();
    let time_since_2000: u64 = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000))
        .try_into()
        .unwrap();

    // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
    let mut data_to_send: Vec<u8> = vec![];
    // Byte1('r'); Identifies the message as a receiver status update.
    data_to_send.extend_from_slice(&[114]); // "r" in ascii

    // The location of the last WAL byte + 1 received and written to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 flushed to disk in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The location of the last WAL byte + 1 applied in the standby.
    data_to_send.extend_from_slice(write_lsn_bytes.as_ref());

    // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    //0, 0, 0, 0, 0, 0, 0, 0,
    data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
    // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
    data_to_send.extend_from_slice(&[1]);

    Bytes::from(data_to_send)
}
