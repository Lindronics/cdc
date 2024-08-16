use postgres_protocol::message::backend::Tuple;
use tokio_postgres::types::ToSql;

pub trait Entity: Send + 'static {
    const TABLE: &'static str;
    const INSERT_SQL: &'static str;
    fn as_args(&self) -> Vec<&(dyn ToSql + Sync)>;
    fn from_tuple(tuple: &Tuple) -> anyhow::Result<Self>
    where
        Self: Sized;
}
