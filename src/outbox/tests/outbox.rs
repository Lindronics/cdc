use ::serde::{Deserialize, Serialize};
use amqp::{Connection, ConnectionProperties, Publish};
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    ExchangeKind,
};
use outbox::model::Message;
use uuid::Uuid;

const MOCK_EXCHANGE: &str = "mock-exchange";
const MOCK_QUEUE: &str = "mock-queue";
const MOCK_ROUTING_KEY: &str = "mock-rk";

#[tokio::test]
async fn outbox_works() {
    let config = outbox::DbConfig {
        host: "localhost".into(),
        port: 5432,
        user: "postgres".into(),
        password: "password".into(),
        dbname: "postgres".into(),
    };
    let amqp_connection =
        Connection::connect("amqp://127.0.0.1:5672", ConnectionProperties::default())
            .await
            .unwrap();
    let consumer_channel = amqp_connection.create_channel().await.unwrap();
    setup_amqp(&consumer_channel).await;

    let (outbox_client, mut handler) = outbox::new::<TestEvent>(&config, &amqp_connection)
        .await
        .unwrap();
    let _bg = tokio::spawn(async move { handler.listen().await });

    let mock_consumer = consumer_channel
        .basic_consume(
            MOCK_QUEUE,
            "mock-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let n = 10;
    tokio::task::spawn(insert_some_records(outbox_client, n));
    consume(mock_consumer, n * 2).await;
}

#[derive(Debug, Serialize, Deserialize)]
struct TestEvent {
    event_id: Uuid,
    agg_id: Uuid,
    payload: String,
}

impl Message for TestEvent {
    fn from_record(record: outbox::model::EventRecord) -> anyhow::Result<Self> {
        Ok(Self {
            event_id: record.id,
            agg_id: record.agg_id,
            payload: String::from_utf8(record.data)?,
        })
    }

    fn into_record(self) -> outbox::model::EventRecord {
        outbox::model::EventRecord {
            id: self.event_id,
            agg_id: self.agg_id,
            data: self.payload.into_bytes(),
            event_type: MOCK_ROUTING_KEY.into(),
            ttl: 3,
        }
    }
}

impl Publish for TestEvent {
    fn exchange(&self) -> &str {
        MOCK_EXCHANGE
    }

    fn routing_key(&self) -> &str {
        MOCK_ROUTING_KEY
    }

    fn payload(&self) -> std::borrow::Cow<'_, [u8]> {
        serde_json::to_vec(self).unwrap().into()
    }
}

async fn insert_some_records(publisher: outbox::client::OutboxClient, n: usize) {
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

async fn setup_amqp(ch: &lapin::Channel) {
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

async fn consume(mut consumer: lapin::Consumer, n: usize) {
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
