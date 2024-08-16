pub enum OrderEvent {
    Created(events::Created),
    Dispatched(events::Dispatched),
    Delivered(events::Delivered),
}

pub mod events {
    use uuid::Uuid;

    pub struct Created {
        pub event_id: Uuid,
        pub order_id: Uuid,
        pub name: String,
    }

    pub struct Dispatched {
        pub event_id: Uuid,
        pub order_id: Uuid,
        pub dispatched_at: String,
    }

    pub struct Delivered {
        pub event_id: Uuid,
        pub order_id: Uuid,
        pub delivered_at: String,
    }
}

impl From<OrderEvent> for outbox::model::EventRecord {
    fn from(value: OrderEvent) -> Self {
        match value {
            OrderEvent::Created(created) => Self {
                id: created.event_id,
                agg_id: created.order_id,
                event_type: "OrderCreated".to_string(),
                data: created.name.into_bytes(),
            },
            OrderEvent::Dispatched(dipatched) => Self {
                id: dipatched.event_id,
                agg_id: dipatched.order_id,
                event_type: "OrderDispatched".to_string(),
                data: dipatched.dispatched_at.into_bytes(),
            },
            OrderEvent::Delivered(delivered) => Self {
                id: delivered.event_id,
                agg_id: delivered.order_id,
                event_type: "OrderDelivered".to_string(),
                data: delivered.delivered_at.into_bytes(),
            },
        }
    }
}
