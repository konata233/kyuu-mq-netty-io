use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex, RwLock};
use crate::mq::io::channel::Channel;
use crate::mq::protocol::proto::DataHead;
use crate::mq::protocol::protobase::Deserialize;

pub struct Session {
    stream: Arc<RwLock<TcpStream>>,
    host: String,
    channels: HashMap<String, Arc<RwLock<Channel>>>,
    self_ref: Option<Arc<RwLock<Session>>>,
    cache: HashMap<String, VecDeque<Box<Vec<u8>>>>
}

impl Session {
    pub fn new<A: ToSocketAddrs>(addr: A, host: String) -> Session {
        Session {
            stream: Arc::new(RwLock::new(TcpStream::connect(addr).unwrap())),
            host,
            channels: HashMap::new(),
            self_ref: None,
            cache: HashMap::new(),
        }
    }

    pub fn init(&mut self, self_ref: Arc<RwLock<Session>>) -> Arc<RwLock<Session>> {
        self.self_ref = Some(self_ref.clone());
        self_ref
    }

    pub fn set_read_timeout(&self, timeout: std::time::Duration) {
        self.stream.write().unwrap().set_read_timeout(Some(timeout)).unwrap();
    }

    pub fn set_write_timeout(&self, timeout: std::time::Duration) {
        self.stream.write().unwrap().set_write_timeout(Some(timeout)).unwrap();
    }

    pub fn create_channel(&mut self, name: String) -> Option<Arc<RwLock<Channel>>> {
        if self.channels.contains_key(&name) {
            return None;
        }
        let channel =Arc::from(RwLock::from(Channel::new(self.host.clone(), name.clone(), self.self_ref.clone()?)));
        self.cache.insert(name.clone(), VecDeque::new());
        self.channels.insert(name.clone(), channel);
        self.channels.get_mut(&name).cloned()
    }

    pub fn drop_channel(&mut self, name: String) {
        let ch = self.channels.get_mut(&name).unwrap();
        if !ch.write().unwrap().is_closed() {
            ch.write().unwrap().close();
        }
        self.cache.remove(&name);
        self.channels.remove(&name);
    }

    pub fn close(&mut self) {
        self.channels.clear();
        self.stream.write().unwrap().shutdown(std::net::Shutdown::Both).unwrap();
    }

    pub fn send(&self, data: Vec<u8>) -> Result<(), std::io::Error> {
        self.stream
            .write()
            .unwrap()
            .write_all(data.as_slice())
    }

    pub fn read(&mut self, channel: &String) -> Result<(Option<DataHead>, Box<Vec<u8>>), Box<dyn Error>> {
        let mut buf_head = [0u8; 256];
        let result = self.stream.write().unwrap().read_exact(&mut buf_head).is_ok();
        if result {
            let head = DataHead::deserialize(buf_head);
            let count = head.slice_count;
            let size = head.slice_size;
            let mut buf = vec![];
            for _ in 0..count {
                let mut buf_slice = vec![0u8; size as usize];
                self.stream.write().unwrap().read_exact(&mut buf_slice)?;
                buf.append(&mut buf_slice);
            }

            Ok((Some(head), Box::from(buf)))
        } else {
            if let Some(cache) = self.cache.get_mut(channel)
                .unwrap()
                .pop_front() {
                Ok((None, cache))
            } else {
                Err("read error".into())
            }
        }
    }

    pub fn send_and_read(&mut self, data: Vec<u8>, channel: &String) -> Result<(Option<DataHead>, Box<Vec<u8>>), Box<dyn Error>> {
        self.stream
            .write()
            .unwrap()
            .write_all(data.as_slice())?;

        self.read(channel)
    }
}