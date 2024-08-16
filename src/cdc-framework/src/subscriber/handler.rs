use std::future::Future;

use crate::db::Entity;

pub trait InsertHandler<T: Entity> {
    fn handle(&self, msg: T) -> impl Future<Output = anyhow::Result<()>> + Send;
}
