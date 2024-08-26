use uuid::Uuid;

pub const ORDER_CREATED: &str = "OrderCreated";
pub const ORDER_DISPATCHED: &str = "OrderDispatched";
pub const ORDER_DELIVERED: &str = "OrderDelivered";

pub const EXCHANGE: &str = "test-exchange";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct OrderEvent {
    pub event_id: Uuid,
    pub order_id: Uuid,
    #[serde(flatten)]
    pub inner: OrderEventInner,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type")]
pub enum OrderEventInner {
    Created(events::Created),
    Dispatched(events::Dispatched),
    Delivered(events::Delivered),
}

pub mod events {

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Created {
        pub name: String,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Dispatched {
        pub dispatched_at: String,
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct Delivered {
        pub delivered_at: String,
    }
}

impl outbox::model::Event for OrderEvent {
    fn from_record(record: outbox::model::EventRecord) -> anyhow::Result<Self> {
        Ok(OrderEvent {
            event_id: record.id,
            order_id: record.agg_id,
            inner: serde_json::from_slice(&record.data).unwrap(),
        })
    }

    fn into_record(self) -> outbox::model::EventRecord {
        outbox::model::EventRecord {
            id: self.event_id,
            agg_id: self.order_id,
            event_type: match self.inner {
                OrderEventInner::Created(_) => ORDER_CREATED.to_string(),
                OrderEventInner::Dispatched(_) => ORDER_DISPATCHED.to_string(),
                OrderEventInner::Delivered(_) => ORDER_DELIVERED.to_string(),
            },
            data: serde_json::to_vec(&self.inner).unwrap(),
        }
    }
}

impl outbox::amqp::Message for OrderEvent {
    fn exchange(&self) -> &str {
        EXCHANGE
    }

    fn routing_key(&self) -> &str {
        match self.inner {
            OrderEventInner::Created(_) => ORDER_CREATED,
            OrderEventInner::Dispatched(_) => ORDER_DISPATCHED,
            OrderEventInner::Delivered(_) => ORDER_DELIVERED,
        }
    }

    fn payload(&self) -> std::borrow::Cow<'_, [u8]> {
        serde_json::to_vec(self).unwrap().into()
    }
}
