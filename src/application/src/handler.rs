pub struct LoggerHandler;

impl outbox::EventHandler<outbox::model::EventRecord> for LoggerHandler {
    async fn handle(&self, msg: outbox::model::EventRecord) -> anyhow::Result<()> {
        println!("{:?}", msg);
        Ok(())
    }
}
