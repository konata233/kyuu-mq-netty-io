pub mod mq;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream};
use mq::protocol;
use crate::mq::protocol::protobase::Serialize;
use crate::mq::protocol::proto::DataHead;
use crate::mq::protocol::protobase::Deserialize;

fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:11451").unwrap();
    let local_addr = stream.local_addr().unwrap();
    let mut route0 = "test_exc".as_bytes().to_vec();
    if route0.len() % 32 != 0 {
        for _ in (0..32 - route0.len() % 32) {
            route0.push(0u8);
        }
    }
    let mut queue_name = "test1".as_bytes().to_vec();
    if queue_name.len() % 32 != 0 {
        for _ in (0..32 - queue_name.len() % 32) {
            queue_name.push(0u8);
        }
    }

    route0.append(&mut [0u8; 64].map(|t| {t}).to_vec());
    route0.append(&mut queue_name);

    let r = <[u8; 128]>::try_from(route0).unwrap();
    let mut header = protocol::proto::DataHead::new(
        String::from("MQ_HOST\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0".to_string()),
        [0u8; 32],
        [1u8, 1u8, 0u8, 0u8],
        [0u8; 24],
        r,
        1,
        256,
        1,
        0
    ).serialize_vec();
    let mut data = String::from("test_exc").as_bytes().to_vec();
    if data.len() % 256 != 0 {
        for _ in (0..256 - data.len() % 256) {
            data.push(0u8);
        }
    }
    header.append(&mut data);
    stream.write(&header).unwrap();
    stream.set_read_timeout(Some(std::time::Duration::from_secs(1))).expect("TODO: panic message");
    loop {
        let mut buf = [0u8; 256];
        match stream.read_exact(&mut buf) {
            Ok(n) => {
                println!("{:?}", protocol::proto::DataHead::deserialize(buf));
            }
            Err(_) => {}
        }
        break;
    }

    stream.shutdown(Shutdown::Both).unwrap();
}


