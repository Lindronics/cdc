use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};

use crate::Publish;

pub struct AmqpPublisher<M> {
    channel: lapin::Channel,
    _config: PublisherConfiguraton,
    message: std::marker::PhantomData<M>,
}

impl<M> AmqpPublisher<M> {
    pub async fn new(connection: &lapin::Connection) -> anyhow::Result<Self> {
        Self::new_with(connection, PublisherConfiguraton::default()).await
    }

    pub async fn new_with(
        connection: &lapin::Connection,
        config: PublisherConfiguraton,
    ) -> anyhow::Result<Self> {
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions {
                nowait: !config.publisher_confirmation,
            })
            .await?;
        Ok(Self {
            channel,
            _config: config,
            message: std::marker::PhantomData,
        })
    }

    pub async fn publish(&self, m: &impl Publish) -> anyhow::Result<()> {
        let confirmation = self
            .channel
            .basic_publish(
                m.exchange(),
                m.routing_key(),
                BasicPublishOptions::default(),
                m.payload().as_ref(),
                m.properties(),
            )
            .await?;

        let confirmation = confirmation.await?;
        anyhow::ensure!(confirmation.is_ack());

        Ok(())
    }
}

impl<M> cdc_framework::EventHandler<outbox::model::EventRecord> for AmqpPublisher<M>
where
    M: outbox::model::Message + Publish + Send + Sync,
{
    async fn handle(&self, msg: outbox::model::EventRecord) -> anyhow::Result<()> {
        let msg = M::from_record(msg)?;
        self.publish(&msg).await
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct PublisherConfiguraton {
    pub publisher_confirmation: bool,
}
