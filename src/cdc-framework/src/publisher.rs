use std::sync::Arc;

use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::db::{self, Entity};

#[derive(Clone)]
pub struct Publisher<T: Entity> {
    client: Arc<RwLock<db::DbClient>>,
    t: std::marker::PhantomData<T>,
}

impl<T: Entity> Publisher<T> {
    pub async fn new(db_client: db::DbClient) -> anyhow::Result<Self> {
        Ok(Self {
            client: Arc::new(RwLock::new(db_client)),
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
