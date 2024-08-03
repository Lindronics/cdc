mod model;

pub struct EventPublisher;

impl cdc_framework::subscriber::handler::EventHandler for EventPublisher {
    async fn handle(&self, msg: cdc_framework::db::EventRecord) -> anyhow::Result<()> {
        println!("{:?}", msg);
        Ok(())
    }
}
