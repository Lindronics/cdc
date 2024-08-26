use postgres_replication::protocol::Tuple;

pub trait Entity: Send + 'static {
    const TABLE: &'static str;

    fn from_tuple(tuple: &Tuple) -> anyhow::Result<Self>
    where
        Self: Sized;
}
