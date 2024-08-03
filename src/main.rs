use cdc::{
    model, publisher,
    subscriber::{handler::PrintHandler, Subscriber},
};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let publisher = publisher::Publisher::new().await.unwrap();

    let mut handler = Subscriber::new(PrintHandler).await.unwrap();
    let _bg = tokio::spawn(async move { handler.listen().await });

    insert_some_records(&publisher).await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

async fn insert_some_records(publisher: &publisher::Publisher) {
    for i in 0..10 {
        publisher
            .persist([
                model::MessageRecord {
                    id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    event_type: format!("event_type {}", i),
                    data: format!("data {}", i),
                },
                model::MessageRecord {
                    id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    event_type: format!("event_type {}", i),
                    data: format!("more data {}", i),
                },
            ])
            .await
            .unwrap();
    }
}
