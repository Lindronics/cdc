use crate::model::{EventRecord, Message};

pub use amqp::{Connection, ConnectionProperties, Publish};

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

    pub async fn publish(&self, event: &Msg) -> anyhow::Result<()>
    where
        Msg: Message + amqp::Publish,
    {
        self.inner.publish(event).await
    }
}

impl<Msg> cdc_framework::InsertHandler<EventRecord> for AmqpPublisher<Msg>
where
    Msg: Message + amqp::Publish + Send + Sync,
{
    async fn handle(&self, msg: EventRecord) -> anyhow::Result<()> {
        self.publish(&Msg::from_record(msg)?).await
    }
}
