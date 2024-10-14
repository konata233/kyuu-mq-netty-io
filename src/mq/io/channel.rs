use std::sync::{Arc, RwLock};
use crate::mq::io::factory::{Command, MessageFactory};
use crate::mq::io::session::Session;
use crate::mq::protocol::proto::DataHead;

pub struct Channel {
    host_name: String,
    name: String,
    closed: bool,
    session: Arc<RwLock<Session>>,
    cache: Vec<Box<Vec<u8>>>,
    cache_count: usize
}

impl Channel {
    pub fn new(host_name: String, name: String, session: Arc<RwLock<Session>>) -> Channel {
        Channel {
            host_name,
            name,
            closed: false,
            session,
            cache: vec![],
            cache_count: 0
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn write_cache(&mut self, data: Box<Vec<u8>>) {
        self.cache.push(data);
        self.cache_count += 1;
    }

    pub fn flush(&mut self) {
        self.get_factory();
        self.cache.clear();
        self.cache_count = 0;
    }

    pub fn read_cache(&mut self) -> Option<Box<Vec<u8>>> {
        self.cache.pop()
    }

    pub fn get_factory(&self) -> MessageFactory {
        MessageFactory::new(self.host_name.clone(), self.name.clone())
    }

    pub fn close(&mut self) {
        let mut factory = self.get_factory();
        let data = factory.command(Command::CloseChannel).build();

        self.send(data);
        self.closed = true;
    }

    pub fn send(&mut self, data: Vec<u8>) {
        self.session.write().unwrap().send(data).unwrap();
    }

    pub fn read(&mut self) -> Result<(DataHead, Vec<u8>), std::io::Error> {
        self.session.write().unwrap().read()
    }

    pub fn send_and_read(&mut self, data: Vec<u8>) -> Result<(DataHead, Vec<u8>), std::io::Error> {
        self.session.write().unwrap().send_and_read(data)
    }
}