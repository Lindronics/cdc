use cdc_framework::db::{DbClient, DbConfig};

fn config() -> DbConfig {
    DbConfig {
        host: "localhost".into(),
        port: 5432,
        user: "postgres".into(),
        password: "password".into(),
        dbname: "postgres".into(),
    }
}

#[tokio::test]
async fn setup_db_replication_enabled() {
    let _client = DbClient::<true>::new(&config()).await.unwrap();
}

#[tokio::test]
async fn setup_db_replication_disabled() {
    let _client = DbClient::<false>::new(&config()).await.unwrap();
}
