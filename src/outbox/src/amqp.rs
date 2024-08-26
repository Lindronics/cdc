use crate::model::{Event, EventRecord};

pub use amqp::*;

pub struct AmqpPublisher<T> {
    inner: amqp::AmqpPublisher,
    t: std::marker::PhantomData<T>,
}

impl<T> AmqpPublisher<T> {
    pub fn new(publisher: amqp::AmqpPublisher) -> Self {
        Self {
            inner: publisher,
            t: std::marker::PhantomData,
        }
    }

    pub async fn publish(&self, event: &T) -> anyhow::Result<()>
    where
        T: Event + amqp::Message,
    {
        self.inner.publish(event).await
    }
}

impl<T> cdc_framework::InsertHandler<EventRecord> for AmqpPublisher<T>
where
    T: Event + amqp::Message + Send + Sync,
{
    async fn handle(&self, msg: EventRecord) -> anyhow::Result<()> {
        println!("Handling msg: {:?}", msg.id);
        self.publish(&T::from_record(msg)?).await
    }
}
