use std::future::Future;

use crate::model::MessageRecord;

pub trait EventHandler {
    fn handle(&self, msg: MessageRecord) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub struct PrintHandler;

impl EventHandler for PrintHandler {
    async fn handle(&self, msg: MessageRecord) -> anyhow::Result<()> {
        println!("{:?}", msg);
        Ok(())
    }
}
