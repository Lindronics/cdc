use std::{borrow::Cow, str::FromStr};

use anyhow::Context;
use postgres_protocol::message::backend::{Tuple, TupleData};
use uuid::Uuid;

#[derive(Debug)]
pub struct MessageRecord {
    pub id: Uuid,
    pub agg_id: Uuid,
    pub event_type: String,
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
            let x = raw_text(row.first().context("missing id")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let agg_id = {
            let x = raw_text(row.get(1).context("missing agg_id")?)?;
            Uuid::from_str(&x).unwrap()
        };
        let event_type = raw_text(row.get(1).context("missing event_type")?)?.into();
        let data = raw_text(row.get(2).context("missing col data")?)?.into();

        Ok(Self {
            id,
            agg_id,
            data,
            event_type,
        })
    }
}
