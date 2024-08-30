use application::model::{self, OrderEvent, OrderEventInner};
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    Connection, ConnectionProperties, ExchangeKind,
};
use uuid::Uuid;

const MOCK_QUEUE: &str = "test-queue";

#[tokio::main]
async fn main() {
    let config = outbox::DbConfig {
        host: "localhost".into(),
        port: 5432,
        user: "postgres".into(),
        password: "password".into(),
        dbname: "postgres".into(),
    };
    let replication_config = outbox::ReplicationConfig {
        table: "events".into(),
        publication: "events_pub".into(),
        replication_slot: "events_slot".into(),
    };
    let amqp_connection =
        Connection::connect("amqp://127.0.0.1:5672", ConnectionProperties::default())
            .await
            .unwrap();
    let consumer_channel = amqp_connection.create_channel().await.unwrap();
    setup_amqp(&consumer_channel).await;

    let (outbox_client, handler) =
        outbox::new::<OrderEvent>(&config, &replication_config, &amqp_connection)
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

async fn insert_some_records(publisher: outbox::client::OutboxClient, n: usize) {
    for i in 0..n {
        publisher
            .persist([
                model::OrderEvent {
                    event_id: Uuid::new_v4(),
                    order_id: Uuid::new_v4(),
                    inner: OrderEventInner::Created(model::events::Created {
                        name: format!("name {}", i),
                    }),
                },
                model::OrderEvent {
                    event_id: Uuid::new_v4(),
                    order_id: Uuid::new_v4(),
                    inner: OrderEventInner::Dispatched(model::events::Dispatched {
                        dispatched_at: "2021-01-01".into(),
                    }),
                },
            ])
            .await
            .unwrap();
    }
}

async fn setup_amqp(ch: &lapin::Channel) {
    ch.exchange_declare(
        model::EXCHANGE,
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
    for rk in [
        model::ORDER_CREATED,
        model::ORDER_DISPATCHED,
        model::ORDER_DELIVERED,
    ] {
        ch.queue_bind(
            MOCK_QUEUE,
            model::EXCHANGE,
            rk,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    }
}

async fn consume(mut consumer: lapin::Consumer, n: usize) {
    for _ in 0..n {
        let Some(delivery) = consumer.next().await else {
            panic!("stream is closed")
        };
        let delivery = delivery.expect("error in consumer");
        delivery.ack(BasicAckOptions::default()).await.expect("ack");

        let event: model::OrderEvent = serde_json::from_slice(&delivery.data).unwrap();
        println!("Consumed event: {}", event.event_id);
    }
}
