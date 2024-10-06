use std::borrow::Cow;

pub trait Publish {
    fn exchange(&self) -> &str;

    fn routing_key(&self) -> &str;

    fn properties(&self) -> lapin::BasicProperties;

    fn payload(&self) -> Cow<'_, [u8]>;
}
