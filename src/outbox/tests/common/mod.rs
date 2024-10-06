pub mod mock_handlers;
pub mod test_event;

use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};
use outbox::{setup, DbClient, DbConfig, ReplicationConfig};
use rand::Rng;
use test_event::TestEvent;
use uuid::Uuid;

pub const MOCK_EXCHANGE: &str = "mock-exchange";
pub const MOCK_QUEUE: &str = "mock-queue";
pub const MOCK_ROUTING_KEY: &str = "mock-rk";

pub struct TestContext {
    pub db_config: DbConfig,
    pub replication_config: ReplicationConfig,
    pub amqp_connection: Connection,
}

impl TestContext {
    pub async fn new() -> Self {
        let db_config = outbox::DbConfig {
            host: "localhost".into(),
            port: 5432,
            user: "postgres".into(),
            password: "password".into(),
            dbname: "postgres".into(),
        };
        let table = format!(
            "x{}",
            rand::thread_rng()
                .sample_iter(rand::distributions::Alphanumeric)
                .take(10)
                .map(char::from)
                .collect::<String>()
        )
        .to_lowercase();
        dbg!(&table);
        let replication_config = outbox::ReplicationConfig {
            publication: format!("{table}_pub"),
            replication_slot: format!("{table}_slot"),
            table,
        };

        let replication_client = DbClient::<true>::new(&db_config).await.unwrap();
        setup(&replication_client, &replication_config.table)
            .await
            .unwrap();

        reqwest::Client::new()
            .put(format!(
                "http://localhost:15672/api/vhosts/{}",
                replication_config.table
            ))
            .basic_auth("guest", Some("guest"))
            .send()
            .await
            .unwrap();

        let amqp_connection = Connection::connect(
            &format!("amqp://127.0.0.1:5672/{}", replication_config.table),
            ConnectionProperties::default(),
        )
        .await
        .unwrap();

        setup_amqp(&amqp_connection.create_channel().await.unwrap()).await;

        Self {
            db_config,
            replication_config,
            amqp_connection,
        }
    }
}

pub async fn insert_some_records(publisher: ::outbox::client::OutboxClient, n: usize) {
    for _ in 0..n {
        publisher
            .persist([
                TestEvent {
                    event_id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    payload: Uuid::new_v4().to_string(),
                },
                TestEvent {
                    event_id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    payload: Uuid::new_v4().to_string(),
                },
            ])
            .await
            .unwrap();
    }
}

pub async fn setup_amqp(ch: &lapin::Channel) {
    ch.exchange_declare(
        MOCK_EXCHANGE,
        ExchangeKind::Topic,
        ExchangeDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_declare(
        MOCK_QUEUE,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
    ch.queue_bind(
        MOCK_QUEUE,
        MOCK_EXCHANGE,
        MOCK_ROUTING_KEY,
        QueueBindOptions::default(),
        FieldTable::default(),
    )
    .await
    .unwrap();
}

pub async fn consume(mut consumer: lapin::Consumer, n: usize) {
    for _ in 0..n {
        let Some(delivery) = consumer.next().await else {
            panic!("stream is closed")
        };
        let delivery = delivery.expect("error in consumer");
        delivery.ack(BasicAckOptions::default()).await.expect("ack");

        let event: TestEvent = serde_json::from_slice(&delivery.data).unwrap();
        println!("Consumed event: {}", event.event_id);
    }
}
