use crate::model::{EventRecord, Message};

pub use amqp::{Connection, ConnectionProperties, Publish};

/// Publishes messages to an AMQP exchange using publisher confirmation.
pub struct AmqpPublisher<Msg> {
    inner: amqp::AmqpPublisher,
    t: std::marker::PhantomData<Msg>,
}

impl<Msg> AmqpPublisher<Msg> {
    pub async fn new(connection: &amqp::Connection) -> anyhow::Result<Self> {
        Ok(Self {
            inner: ::amqp::AmqpPublisher::new(connection).await?,
            t: std::marker::PhantomData,
        })
    }
}

impl<Msg> cdc_framework::EventHandler<EventRecord> for AmqpPublisher<Msg>
where
    Msg: Message + amqp::Publish + Send + Sync,
{
    async fn handle(&self, msg: EventRecord) -> anyhow::Result<()> {
        let event: &Msg = &Msg::from_record(msg)?;
        self.inner.publish(event).await
    }
}
