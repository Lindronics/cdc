use std::{borrow::Cow, future::Future, str::FromStr};

use anyhow::Context;
use postgres_protocol::message::backend::{Tuple, TupleData};
use uuid::Uuid;

#[derive(Debug)]
pub struct MessageRecord {
    pub id: Uuid,
    pub agg_id: Uuid,
    pub data: String,
}

impl TryFrom<&Tuple> for MessageRecord {
    type Error = anyhow::Error;

    fn try_from(value: &Tuple) -> Result<Self, Self::Error> {
        fn raw_text(data: &TupleData) -> anyhow::Result<Cow<str>> {
            match data {
                TupleData::Text(x) => Ok(String::from_utf8_lossy(x)),
                _ => anyhow::bail!("expected text"),
            }
        }

        let row = value.tuple_data();

        let id = {
            let x = raw_text(row.first().context("missing col 0")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let agg_id = {
            let x = raw_text(row.get(1).context("missing col 1")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let data = raw_text(row.get(2).context("missing col 2")?)?.into();
        Ok(Self { id, agg_id, data })
    }
}

pub trait MessageHandler {
    fn handle(&self, msg: MessageRecord) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub struct PrintHandler;

impl MessageHandler for PrintHandler {
    async fn handle(&self, msg: MessageRecord) -> anyhow::Result<()> {
        println!("{:?}", msg);
        Ok(())
    }
}
