use std::io::Write;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use crate::mq::io::factory::MessageFactory;

pub struct Channel {
    host_name: String,
    name: String,
    stream: Arc<Mutex<TcpStream>>
}

impl Channel {
    pub fn new(host_name: String, name: String, stream: Arc<Mutex<TcpStream>>) -> Channel {
        Channel {
            host_name,
            name,
            stream
        }
    }

    pub fn get_factory(&self) -> MessageFactory {
        MessageFactory::new(self.host_name.clone(), self.name.clone())
    }

    pub fn close(&self) {
        // todo: this method should send a 'close channel' message to the remote server.
    }

    pub fn send_and_read(&self, data: Vec<u8>) {
        self.stream
            .lock()
            .unwrap()
            .write_all(data.as_slice())
            .unwrap();

        let mut buf_head = [0u8; 256];
    }
}