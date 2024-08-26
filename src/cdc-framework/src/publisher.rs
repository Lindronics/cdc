use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

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

    pub async fn as_ref(&self) -> RwLockReadGuard<'_, db::DbClient> {
        self.client.read().await
    }

    pub async fn as_mut(&self) -> RwLockWriteGuard<'_, db::DbClient> {
        self.client.write().await
    }
}
