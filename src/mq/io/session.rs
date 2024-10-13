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

    pub fn create_channel(&mut self, name: String) -> Option<&mut Channel> {
        let channel = Channel::new(self.host.clone(), name.clone(), self.stream.clone());
        self.channels.insert(name.clone(), channel);
        self.channels.get_mut(&name)
    }

    pub fn drop_channel(&mut self, name: String) {
        self.channels.remove(&name);
    }

    pub fn close(&mut self) {
        self.channels.clear();
        self.stream.lock().unwrap().shutdown(std::net::Shutdown::Both).unwrap();
    }
}