use std::ops::{Deref, DerefMut};

use tokio_postgres::NoTls;

mod config;
mod model;
mod setup;

pub use config::{DbConfig, ReplicationConfig};
pub use model::Entity;

pub struct DbClient<const REPLICATION: bool = false> {
    pub dbname: String,
    client: tokio_postgres::Client,
}

impl<const REPLICATION: bool> DbClient<REPLICATION> {
    pub async fn new(config: &DbConfig) -> anyhow::Result<Self> {
        let (client, connection) =
            tokio_postgres::connect(&config.connection_string(REPLICATION), NoTls)
                .await
                .unwrap();
        tokio::spawn(connection);

        Ok(Self {
            client,
            dbname: config.dbname.clone(),
        })
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
