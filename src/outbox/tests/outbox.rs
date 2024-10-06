use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use amqp::AmqpPublisher;
use lapin::{options::BasicConsumeOptions, types::FieldTable};

mod common;

use common::{
    consume, insert_some_records, mock_handlers, test_event::TestEvent, TestContext, MOCK_QUEUE,
};
use outbox::{client::OutboxClient, handlers, subscriber::OutboxSubscriber};

#[tokio::test]
async fn outbox_works() {
    let context = TestContext::new().await;

    let client = OutboxClient::new(&context.db_config, &context.replication_config)
        .await
        .unwrap();
    let amqp_publisher = AmqpPublisher::<TestEvent>::new(&context.amqp_connection)
        .await
        .unwrap();

    let sub = OutboxSubscriber::new(
        &context.db_config,
        &context.replication_config,
        amqp_publisher,
    )
    .await
    .unwrap();

    let _bg = tokio::spawn(async move { sub.listen().await });
    let mock_consumer = context
        .amqp_connection
        .create_channel()
        .await
        .unwrap()
        .basic_consume(
            MOCK_QUEUE,
            "mock-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let n = 10;
    insert_some_records(client, n).await;
    consume(mock_consumer, n * 2).await;
}

#[tokio::test]
async fn error_gets_retried() {
    let context = TestContext::new().await;

    let client = OutboxClient::new(&context.db_config, &context.replication_config)
        .await
        .unwrap();

    // Add a handler that fails until TTL is down to 1
    let total_attempts = Arc::new(AtomicU32::new(0));
    let handler = {
        let amqp_publisher = amqp::AmqpPublisher::<TestEvent>::new(&context.amqp_connection)
            .await
            .unwrap();
        let fallible_handler = mock_handlers::FallibleHandler {
            succeed_on: 1,
            attempts: total_attempts.clone(),
            inner: amqp_publisher,
        };
        handlers::EagerRetryHandler::new(client.clone(), fallible_handler)
            .await
            .unwrap()
    };

    let sub = OutboxSubscriber::new(&context.db_config, &context.replication_config, handler)
        .await
        .unwrap();

    let _bg = tokio::spawn(async move { sub.listen().await });

    let mock_consumer = context
        .amqp_connection
        .create_channel()
        .await
        .unwrap()
        .basic_consume(
            MOCK_QUEUE,
            "mock-consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let n = 2;
    insert_some_records(client, n).await;
    consume(mock_consumer, n * 2).await;

    assert_eq!(total_attempts.load(Ordering::Relaxed), 12);
}
