use std::borrow::Cow;

use lapin::{
    options::{BasicPublishOptions, ConfirmSelectOptions},
    BasicProperties,
};

pub use lapin::{Connection, ConnectionProperties};

pub trait Message {
    fn exchange(&self) -> &str;

    fn routing_key(&self) -> &str;

    fn payload(&self) -> Cow<'_, [u8]>;
}

pub struct AmqpPublisher {
    channel: lapin::Channel,
}

impl AmqpPublisher {
    pub async fn new(connection: &lapin::Connection) -> anyhow::Result<Self> {
        let channel = connection.create_channel().await?;
        channel
            .confirm_select(ConfirmSelectOptions { nowait: false })
            .await?;
        Ok(Self { channel })
    }

    pub async fn publish(&self, m: &impl Message) -> anyhow::Result<()> {
        let confirmation = self
            .channel
            .basic_publish(
                m.exchange(),
                m.routing_key(),
                BasicPublishOptions::default(),
                m.payload().as_ref(),
                BasicProperties::default(),
            )
            .await?;

        let confirmation = confirmation.await?;
        anyhow::ensure!(confirmation.is_ack());

        Ok(())
    }
}
