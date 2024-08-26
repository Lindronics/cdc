use std::{
    pin::Pin,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use handler::InsertHandler;
use postgres_replication::protocol::{CommitBody, LogicalReplicationMessage, ReplicationMessage};
use tokio_postgres::{types::PgLsn, SimpleQueryMessage};

use crate::db::{self, Entity};

pub mod handler;

pub struct Subscriber<T: Entity, H: InsertHandler<T>> {
    stream: Pin<Box<tokio_postgres::CopyBothDuplex<bytes::Bytes>>>,
    message_handler: Arc<H>,
    t: std::marker::PhantomData<T>,
}

impl<T, H> Subscriber<T, H>
where
    T: Entity,
    H: InsertHandler<T> + Send + Sync + 'static,
{
    pub async fn new(db_client: &db::DbClient<true>, message_handler: H) -> anyhow::Result<Self> {
        db_client.setup::<T>().await?;
        let lsn = get_start_lsn(db_client, T::TABLE).await?;

        let stream = db_client
            .copy_both_simple::<bytes::Bytes>(
                &(format!(
                    r#"
                    START_REPLICATION SLOT {table}_slot 
                    LOGICAL {lsn} 
                    (
                        "proto_version" '1', 
                        "publication_names" '{table}_pub'
                    );
                    "#,
                    table = T::TABLE,
                )),
            )
            .await?;

        Ok(Self {
            stream: Box::pin(stream),
            message_handler: Arc::new(message_handler),
            t: std::marker::PhantomData,
        })
    }

    pub async fn listen(&mut self) -> anyhow::Result<()> {
        let mut futures = vec![];
        while let Some(msg) = self.stream.as_mut().next().await {
            let msg = msg.context("could not get next message in stream")?;

            let ReplicationMessage::XLogData(data) = ReplicationMessage::parse(&msg)? else {
                continue;
            };

            match LogicalReplicationMessage::parse(data.data())? {
                // Process INSERTS in the background
                LogicalReplicationMessage::Insert(msg) => {
                    let record = T::from_tuple(msg.tuple())?;
                    let event_handler = self.message_handler.clone();
                    futures.push(tokio::spawn(
                        async move { event_handler.handle(record).await },
                    ));
                }
                // On COMMIT, finish processing all the INSERTS before ACKing the whole transaction
                LogicalReplicationMessage::Commit(msg) => {
                    futures::future::try_join_all(std::mem::take(&mut futures))
                        .await
                        .context("failed to process msg, aborting")?;
                    self.ack(msg).await?;
                }
                _ => {
                    continue;
                }
            };
        }
        Ok(())
    }

    async fn ack(&mut self, commit: CommitBody) -> anyhow::Result<()> {
        let ssu = prepare_ssu(PgLsn::from(commit.end_lsn()));
        self.stream.as_mut().send(ssu).await?;
        println!("- ACKED");
        Ok(())
    }
}

async fn get_start_lsn(client: &db::DbClient<true>, table: &str) -> anyhow::Result<PgLsn> {
    let result = client
        .simple_query(&format!(
            r#"
            SELECT confirmed_flush_lsn
            FROM pg_replication_slots
            WHERE slot_name = '{table}_slot'
            "#,
        ))
        .await?;

    let row = result
        .into_iter()
        .find_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .context("empty rows")?;

    let lsn = row
        .get("confirmed_flush_lsn")
        .context("missing confirmed_flush_lsn")?
        .to_string()
        .parse()
        .map_err(|_| anyhow::anyhow!("failed to parse LSN"))?;

    Ok(lsn)
}

// https://github.com/tablelandnetwork/pglogrepl-rust/blob/5fb7b8d55d07246077898489c18361d71c835b7b/src/replication.rs#L109
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
