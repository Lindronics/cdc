use std::string::String;

use crate::model::events;

#[derive(Clone, prost::Message)]
pub struct OrderCreated {
    #[prost(string, tag = "1")]
    pub order_id: String,
    #[prost(string, tag = "2")]
    pub event_id: String,
    #[prost(string, tag = "3")]
    pub name: String,
}

impl From<events::Created> for OrderCreated {
    fn from(event: events::Created) -> Self {
        Self {
            event_id: event.event_id.to_string(),
            order_id: event.order_id.to_string(),
            name: event.name.to_string(),
        }
    }
}
