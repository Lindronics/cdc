use ::outbox::model::Message;
use ::serde::{Deserialize, Serialize};
use amqp::Publish;
use uuid::Uuid;

use super::{MOCK_EXCHANGE, MOCK_ROUTING_KEY};

#[derive(Debug, Serialize, Deserialize)]
pub struct TestEvent {
    pub event_id: Uuid,
    pub agg_id: Uuid,
    pub payload: String,
}

impl Message for TestEvent {
    fn from_record(record: ::outbox::model::EventRecord) -> anyhow::Result<Self> {
        Ok(Self {
            event_id: record.id,
            agg_id: record.agg_id,
            payload: String::from_utf8(record.data)?,
        })
    }

    fn into_record(self) -> ::outbox::model::EventRecord {
        ::outbox::model::EventRecord {
            id: self.event_id,
            agg_id: self.agg_id,
            data: self.payload.into_bytes(),
            event_type: MOCK_ROUTING_KEY.into(),
            ttl: 3,
        }
    }
}

impl Publish for TestEvent {
    fn exchange(&self) -> &str {
        MOCK_EXCHANGE
    }

    fn routing_key(&self) -> &str {
        MOCK_ROUTING_KEY
    }

    fn payload(&self) -> std::borrow::Cow<'_, [u8]> {
        serde_json::to_vec(self).unwrap().into()
    }
}
