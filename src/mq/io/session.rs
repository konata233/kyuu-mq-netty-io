use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use crate::mq::io::channel::Channel;

pub struct Session {
    stream: Arc<Mutex<TcpStream>>,
    host: String,
    channels: HashMap<String, Channel>
}

impl Session {
    pub fn new<A: ToSocketAddrs>(addr: A, host: String) -> Session {
        Session {
            stream: Arc::new(Mutex::new(TcpStream::connect(addr).unwrap())),
            host,
            channels: HashMap::new()
        }
    }

    pub fn set_read_timeout(&self, timeout: std::time::Duration) {
        self.stream.lock().unwrap().set_read_timeout(Some(timeout)).unwrap();
    }

    pub fn set_write_timeout(&self, timeout: std::time::Duration) {
        self.stream.lock().unwrap().set_write_timeout(Some(timeout)).unwrap();
    }

    pub fn create_channel(&mut self, name: String) -> Option<&mut Channel> {
        if self.channels.contains_key(&name) {
            return None;
        }
        let channel = Channel::new(self.host.clone(), name.clone(), self.stream.clone());
        self.channels.insert(name.clone(), channel);
        self.channels.get_mut(&name)
    }

    pub fn drop_channel(&mut self, name: String) {
        let ch = self.channels.get_mut(&name).unwrap();
        if !ch.is_closed() {
            ch.close();
        }
        self.channels.remove(&name);
    }

    pub fn close(&mut self) {
        self.channels.clear();
        self.stream.lock().unwrap().shutdown(std::net::Shutdown::Both).unwrap();
    }
}