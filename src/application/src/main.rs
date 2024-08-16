use application::events_publisher::EventPublisher;
use cdc_framework::{
    db::{self, DbClient},
    publisher,
    subscriber::Subscriber,
};
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let config = db::DbConfig {
        host: "localhost".into(),
        port: 5432,
        user: "postgres".into(),
        password: "password".into(),
        dbname: "postgres".into(),
    };
    let publisher = publisher::Publisher::new(DbClient::new(&config).await.unwrap())
        .await
        .unwrap();

    let mut handler = Subscriber::new(&DbClient::new(&config).await.unwrap(), EventPublisher)
        .await
        .unwrap();
    let _bg = tokio::spawn(async move { handler.listen().await });

    insert_some_records(&publisher).await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

async fn insert_some_records(publisher: &publisher::Publisher) {
    for i in 0..10 {
        publisher
            .persist([
                db::EventRecord {
                    id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    event_type: format!("event_type {}", i),
                    data: format!("data {}", i).into_bytes(),
                },
                db::EventRecord {
                    id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    event_type: format!("event_type {}", i),
                    data: format!("more data {}", i).into_bytes(),
                },
            ])
            .await
            .unwrap();
    }
}
