use anyhow::Context;
use tokio::sync::RwLock;

use crate::db::{self, Entity};

pub struct Publisher<T: Entity> {
    client: RwLock<db::DbClient>,
    t: std::marker::PhantomData<T>,
}

impl<T: Entity> Publisher<T> {
    pub async fn new(db_client: db::DbClient) -> anyhow::Result<Self> {
        db_client.setup::<T>().await?;
        Ok(Self {
            client: RwLock::new(db_client),
            t: std::marker::PhantomData,
        })
    }

    pub async fn persist_one(&self, item: T) -> anyhow::Result<()> {
        let client = self
            .client
            .try_read()
            .context("failed to acquire read lock")?;

        client.execute(T::INSERT_SQL, &item.as_args()).await?;

        Ok(())
    }
    pub async fn persist(&self, items: impl IntoIterator<Item = T>) -> anyhow::Result<()> {
        let mut client_mut = self
            .client
            .try_write()
            .context("failed to acquire write lock")?;

        let transaction = client_mut.transaction().await?;
        for item in items {
            transaction.execute(T::INSERT_SQL, &item.as_args()).await?;
        }
        transaction.commit().await?;

        Ok(())
    }
}
