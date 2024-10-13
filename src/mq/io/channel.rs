use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use crate::mq::io::factory::{Command, DataType, MessageFactory};
use crate::mq::protocol::proto::DataHead;
use crate::mq::protocol::protobase::Deserialize;

pub struct Channel {
    host_name: String,
    name: String,
    stream: Arc<Mutex<TcpStream>>,
    closed: bool
}

impl Channel {
    pub fn new(host_name: String, name: String, stream: Arc<Mutex<TcpStream>>) -> Channel {
        Channel {
            host_name,
            name,
            stream,
            closed: false
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn set_read_timeout(&self, timeout: std::time::Duration) {
        self.stream.lock().unwrap().set_read_timeout(Some(timeout)).unwrap();
    }

    pub fn set_write_timeout(&self, timeout: std::time::Duration) {
        self.stream.lock().unwrap().set_write_timeout(Some(timeout)).unwrap();
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

    pub fn send(&self, data: Vec<u8>) {
        self.stream
            .lock()
            .unwrap()
            .write_all(data.as_slice())
            .unwrap();
    }

    pub fn send_and_read(&self, data: Vec<u8>) -> (DataHead, Vec<u8>) {
        self.stream
            .lock()
            .unwrap()
            .write_all(data.as_slice())
            .unwrap();

        let mut buf_head = [0u8; 256];
        self.stream.lock().unwrap().read_exact(&mut buf_head).unwrap();
        let head = DataHead::deserialize(buf_head);
        let count = head.slice_count;
        let size = head.slice_size;
        let mut buf = vec![];
        for _ in 0..count {
            let mut buf_slice = vec![0u8; size as usize];
            self.stream.lock().unwrap().read_exact(&mut buf_slice).unwrap();
            buf.append(&mut buf_slice);
        }
        (head, buf)
    }
}