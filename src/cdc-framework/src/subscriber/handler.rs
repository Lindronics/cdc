use std::future::Future;

use crate::db::EventRecord;

pub trait EventHandler {
    fn handle(&self, msg: EventRecord) -> impl Future<Output = anyhow::Result<()>> + Send;
}
