use core::str;
use std::{borrow::Cow, str::FromStr};

use anyhow::Context;
use cdc_framework::db::Entity;
use postgres_replication::protocol::{Tuple, TupleData};
use uuid::Uuid;

pub trait Message: Sized {
    fn from_record(record: EventRecord) -> anyhow::Result<Self>;

    fn into_record(self) -> EventRecord;
}

#[derive(Debug)]
pub struct EventRecord {
    pub id: Uuid,
    pub agg_id: Uuid,
    pub event_type: String,
    pub data: Vec<u8>,
    pub ttl: i16,
}

impl Entity for EventRecord {
    const TABLE: &'static str = "events";

    fn from_tuple(tuple: &Tuple) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Self::try_from(tuple)
    }
}

impl TryFrom<&Tuple> for EventRecord {
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
            let x = raw_text(row.first().context("missing id")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let agg_id = {
            let x = raw_text(row.get(1).context("missing agg_id")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let event_type = raw_text(row.get(2).context("missing event_type")?)?.into();
        let data = match row.get(3).context("missing col data")? {
            TupleData::Text(x) => hex::decode(x.slice(2..))?,
            _ => anyhow::bail!("expected text"),
        };
        let ttl = raw_text(row.get(4).context("missing col ttl")?)?.parse()?;

        Ok(Self {
            id,
            agg_id,
            data,
            event_type,
            ttl,
        })
    }
}
