use crate::mq::api::queue::Queue;
use crate::mq::routing::chain::RoutingChain;

pub trait ChannelApi {
    fn create_exchange(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn std::error::Error>>;
    fn create_queue(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn std::error::Error>>;
    fn drop_exchange(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn std::error::Error>>;
    fn drop_queue(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn std::error::Error>>;
}

pub trait ChannelQueueApi {
    fn get_queue(&mut self, routing_chain: RoutingChain) -> Result<Queue, Box<dyn std::error::Error>>;
}

pub enum FetchResult {
    Success(Vec<u8>),
    FailedNoItem,
    FailedEmptyMessage,
    FailedError(Box<dyn std::error::Error>),
}

pub enum FetchResultString {
    Success(String),
    FailedNotUtf8,
    FailedNoItem,
    FailedEmptyMessage,
    FailedError(Box<dyn std::error::Error>),
}