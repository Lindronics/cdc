use amqp::Publish;
use lapin::{
    options::ExchangeDeclareOptions, types::FieldTable, ConnectionProperties, ExchangeKind,
};

struct TestMsg(String);

impl Publish for TestMsg {
    fn exchange(&self) -> &str {
        "test-exchange"
    }

    fn routing_key(&self) -> &str {
        "test-routing-key"
    }

    fn payload(&self) -> std::borrow::Cow<'_, [u8]> {
        self.0.as_bytes().into()
    }
}

#[tokio::test]
async fn publish() {
    let msg = TestMsg("Hello, world!".to_string());

    let connection =
        lapin::Connection::connect("amqp://127.0.0.1:5672", ConnectionProperties::default())
            .await
            .unwrap();
    let channel = connection.create_channel().await.unwrap();
    channel
        .exchange_declare(
            msg.exchange(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let publisher = amqp::AmqpPublisher::new(&connection).await.unwrap();
    publisher.publish(&msg).await.unwrap();
}
