use application::handler::LoggerHandler;
use uuid::Uuid;

#[tokio::main]
async fn main() {
    let config = outbox::DbConfig {
        host: "localhost".into(),
        port: 5432,
        user: "postgres".into(),
        password: "password".into(),
        dbname: "postgres".into(),
    };
    let client = outbox::DbClient::new(&config).await.unwrap();
    outbox::setup(&client).await.unwrap();
    let publisher = outbox::Publisher::new(client).await.unwrap();

    let mut handler = outbox::Subscriber::new(
        &outbox::DbClient::new(&config).await.unwrap(),
        LoggerHandler,
    )
    .await
    .unwrap();
    let _bg = tokio::spawn(async move { handler.listen().await });

    insert_some_records(&publisher).await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
}

async fn insert_some_records(publisher: &outbox::Publisher) {
    for i in 0..10 {
        publisher
            .persist([
                outbox::model::EventRecord {
                    id: Uuid::new_v4(),
                    agg_id: Uuid::new_v4(),
                    event_type: format!("event_type {}", i),
                    data: format!("data {}", i).into_bytes(),
                },
                outbox::model::EventRecord {
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
