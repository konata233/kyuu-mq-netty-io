use crate::mq::api::common::{FetchResult, FetchResultString};
use crate::mq::io::factory::{DataType, MessageFactory, MessageType, RoutingModFactory, RoutingType};
use crate::mq::io::session::Session;
use crate::mq::protocol::proto::DataHead;
use crate::mq::routing::chain::RoutingChain;
use std::sync::{Arc, RwLock};

pub struct Queue {
    routing_chain: RoutingChain,
    channel_name: String,
    host_name: String,
    session: Arc<RwLock<Session>>,
}

impl Queue {
    pub fn new(routing_chain: RoutingChain, channel_name: String, host_name: String, session: Arc<RwLock<Session>>) -> Queue {
        Queue {
            routing_chain,
            channel_name,
            host_name,
            session,
        }
    }

    pub fn push(&self, data: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let routing_mod = RoutingModFactory::new()
            .data_type(DataType::Message)
            .message_type(MessageType::Push)
            .routing_type(RoutingType::Direct)
            .build();

        Ok(self.session.write().unwrap().send(
            MessageFactory::new(self.host_name.clone(), self.channel_name.clone())
                .routing_mod(routing_mod)
                .routing_chain(self.routing_chain.clone())
                .data(data)
                .build()
        )?)
    }

    pub fn fetch(&self) -> Result<(Option<DataHead>, Option<Vec<u8>>), Box<dyn std::error::Error>> {
        let routing_mod = RoutingModFactory::new()
            .data_type(DataType::Message)
            .message_type(MessageType::Fetch)
            .routing_type(RoutingType::Direct)
            .build();

        let (head, data) = self.session.write().unwrap().send_and_read(
            MessageFactory::new(self.host_name.clone(), self.channel_name.clone())
                .routing_mod(routing_mod)
                .routing_chain(self.routing_chain.clone())
                .build(),
            &self.channel_name
        )?;

        Ok((head, Some(*data)))
    }

    pub fn push_string(&self, data: String) -> Result<(), Box<dyn std::error::Error>> {
        self.push(data.into_bytes())
    }

    pub fn fetch_string(&self) -> Result<(Option<DataHead>, Option<String>), Box<dyn std::error::Error>> {
        let (head, data) = self.fetch()?;
        Ok((head, data.map(|data| String::from_utf8(data).unwrap())))
    }

    pub fn fetch_simple(&self) -> FetchResult {
        match self.fetch() {
            Ok((head, data)) =>{
                if head.is_some() && data.is_some() {
                    if head.unwrap().errcode == 0x0u16 {
                        FetchResult::Success(data.unwrap())
                    } else {
                        FetchResult::FailedNoItem
                    }
                } else {
                    FetchResult::FailedEmptyMessage
                }
            }
            Err(err) => {
                FetchResult::FailedError(err)
            }
        }
    }

    pub fn fetch_simple_string(&self) -> FetchResultString {
        match self.fetch_simple() {
            FetchResult::Success(data) =>
                if let Ok(mut s) = String::from_utf8(data) {
                    s = s.trim_end_matches('\0')
                        .trim()
                        .to_string();
                    FetchResultString::Success(s)
                } else {
                    FetchResultString::FailedNotUtf8
                }
            FetchResult::FailedNoItem => {
                FetchResultString::FailedNoItem
            }
            FetchResult::FailedEmptyMessage => {
                FetchResultString::FailedEmptyMessage
            }
            FetchResult::FailedError(err) => {
                FetchResultString::FailedError(err)
            }
        }
    }
}