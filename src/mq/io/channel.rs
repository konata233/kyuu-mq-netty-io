use crate::mq::api::common::{ChannelApi, ChannelQueueApi};
use crate::mq::io::factory::{Command, CommandType, DataType, MessageFactory, RoutingModFactory, RoutingType};
use crate::mq::io::session::Session;
use crate::mq::protocol::proto::DataHead;
use crate::mq::routing::chain::RoutingChain;
use std::error::Error;
use std::sync::{Arc, RwLock};
use crate::mq::api::queue::Queue;

pub struct Channel {
    host_name: String,
    name: String,
    closed: bool,
    session: Arc<RwLock<Session>>,
}

impl Channel {
    pub fn new(host_name: String, name: String, session: Arc<RwLock<Session>>) -> Channel {
        Channel {
            host_name,
            name,
            closed: false,
            session,
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }


    pub fn get_factory(&self) -> MessageFactory {
        MessageFactory::new(self.host_name.clone(), self.name.clone())
    }

    pub fn close(&mut self) {
        let factory = self.get_factory();
        let data = factory.command(Command::CloseChannel).build();

        let _ = self.send(data);
        self.closed = true;
    }

    pub fn send(&mut self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        Ok(self.session.write().unwrap().send(data)?)
    }

    pub fn read(&mut self) -> Result<(Option<DataHead>, Box<Vec<u8>>), Box<dyn Error>> {
        self.session.write().unwrap().read(&self.name)
    }

    pub fn send_and_read(&mut self, data: Vec<u8>) -> Result<(Option<DataHead>, Box<Vec<u8>>), Box<dyn Error>> {
        self.session.write().unwrap().send_and_read(data, &self.name)
    }
}

impl ChannelApi for Channel {
    fn create_exchange(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn Error>> {
        let routing_mod = RoutingModFactory::new()
            .routing_type(RoutingType::Direct)
            .data_type(DataType::Command)
            .command_type(CommandType::NewExchange)
            .build();
        self.send(
            self.get_factory()
                .routing_mod(routing_mod)
                .routing_chain(routing_chain)
                .data(name.into_bytes())
                .build()
        ).into()
    }

    fn create_queue(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn Error>> {
        let routing_mod = RoutingModFactory::new()
            .routing_type(RoutingType::Direct)
            .data_type(DataType::Command)
            .command_type(CommandType::NewQueue)
            .build();
        self.send(
            self.get_factory()
                .routing_mod(routing_mod)
                .routing_chain(routing_chain)
                .data(name.into_bytes())
                .build()
        ).into()
    }

    fn drop_exchange(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn Error>> {
        let routing_mod = RoutingModFactory::new()
            .routing_type(RoutingType::Direct)
            .data_type(DataType::Command)
            .command_type(CommandType::DropExchange)
            .build();
        self.send(
            self.get_factory()
                .routing_mod(routing_mod)
                .routing_chain(routing_chain)
                .data(name.into_bytes())
                .build()
        ).into()
    }

    fn drop_queue(&mut self, name: String, routing_chain: RoutingChain) -> Result<(), Box<dyn Error>> {
        let routing_mod = RoutingModFactory::new()
            .routing_type(RoutingType::Direct)
            .data_type(DataType::Command)
            .command_type(CommandType::DropQueue)
            .build();
        self.send(
            self.get_factory()
                .routing_mod(routing_mod)
                .routing_chain(routing_chain)
                .data(name.into_bytes())
                .build()
        ).into()
    }
}

impl ChannelQueueApi for Channel {
    fn get_queue(&mut self, routing_chain: RoutingChain) -> Result<Queue, Box<dyn Error>> {
        Ok(Queue::new(routing_chain, self.name.clone(), self.host_name.clone(), self.session.clone()))
    }
}