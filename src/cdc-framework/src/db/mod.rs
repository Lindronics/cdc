use std::ops::{Deref, DerefMut};

use tokio_postgres::NoTls;

mod config;
mod model;
mod setup;

pub use config::DbConfig;
pub use model::EventRecord;

pub struct DbClient<const REPLICATION: bool = false> {
    client: tokio_postgres::Client,
}

impl<const REPLICATION: bool> DbClient<REPLICATION> {
    pub async fn new(config: &DbConfig) -> anyhow::Result<Self> {
        let (client, connection) =
            tokio_postgres::connect(&config.connection_string(REPLICATION), NoTls)
                .await
                .unwrap();
        tokio::spawn(connection);

        setup::setup_db(&client).await?;

        Ok(Self { client })
    }
}

impl<const REPLICATION: bool> Deref for DbClient<REPLICATION> {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<const REPLICATION: bool> DerefMut for DbClient<REPLICATION> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}
