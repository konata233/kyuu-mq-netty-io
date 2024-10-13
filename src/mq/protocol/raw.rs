use crate::mq::routing::key::RoutingKey;

#[derive(Debug)]
pub struct RawData {
    pub raw: Raw,
    pub channel: String,
    pub virtual_host: String,
    pub routing_key: RoutingKey
}

#[derive(Debug)]
pub enum Raw {
    Message(RawMessage),
    Command(RawCommand),
    Nop
}

#[derive(Debug)]
pub enum RawMessage {
    Push(Vec<u8>),
    Fetch(Vec<u8>),
    Nop
}

#[derive(Debug)]
pub enum RawCommand {
    NewQueue(Vec<u8>),
    NewExchange(Vec<u8>),
    NewBinding(Vec<u8>),

    DropQueue(Vec<u8>),
    DropExchange(Vec<u8>),
    DropBinding(Vec<u8>),

    Nop
}