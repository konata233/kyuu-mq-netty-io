pub mod mq;

use std::io::{Read, Write};
use std::sync::{Arc, RwLock};
use std::thread;
use crate::mq::io::factory::{CommandType, DataType, MessageType, Routing, RoutingMod, RoutingModFactory, RoutingType};
use crate::mq::io::session;
use crate::mq::protocol::proto::DataHead;
use crate::mq::protocol::protobase::Serialize;
use crate::mq::protocol::protobase::Deserialize;

fn main() {
    // todo: refactor the implementation of channels; use session to control net io instead.
    let mut session = session::Session::new("127.0.0.1:11451", "MQ_HOST".to_string());
    let mut session = Arc::from(RwLock::from(session));
    session
        .clone()
        .write()
        .unwrap()
        .init(session.clone());

    println!("Conn established!");
    session.write().unwrap().set_read_timeout(std::time::Duration::from_millis(800));
    let channel = session.write().unwrap().create_channel("MQ_CHANNEL".to_string()).clone().unwrap();
    let channel2 = session.write().unwrap().create_channel("MQ_CHANNEL2".to_string()).clone().unwrap();
    let channel3 = channel2.clone();

    let msg = channel.read().unwrap().get_factory()
        .routing_mod(RoutingModFactory::new()
            .data_type(DataType::Command)
            .command_type(CommandType::NewExchange)
            .routing_type(RoutingType::Direct)
            .build()
        )
        .route(Routing::Stop)
        .data(String::from("base_exc").into_bytes())
        .build();
    channel.write().unwrap().send(msg);

    let msg = channel.read().unwrap().get_factory()
        .routing_mod(RoutingModFactory::new()
            .data_type(DataType::Command)
            .command_type(CommandType::NewQueue)
            .routing_type(RoutingType::Direct)
            .build()
        )
        .route(Routing::Route(String::from("base_exc")))
        .route(Routing::Stop)
        .data(String::from("base_queue").into_bytes())
        .build();
    channel.write().unwrap().send(msg);

    let handle = thread::spawn(move || {
        for i in 1..100 {
            let routing = RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Push)
                .routing_type(RoutingType::Direct)
                .build();

            let msg = channel2.read().unwrap().get_factory()
                .routing_mod(routing)
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .data(String::from(format!("hello world from thread 2: {}", i)).into_bytes())
                .build();
            channel2.write().unwrap().send(msg);
            println!("Writing thread 1!")
        }
    });

    for i in 1..100 {
        let msg = channel.read().unwrap().get_factory()
            .routing_mod(RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Push)
                .routing_type(RoutingType::Direct)
                .build()
            )
            .route(Routing::Route(String::from("base_exc")))
            .route(Routing::Stop)
            .queue_name(String::from("base_queue"))
            .data(String::from(format!("hello world from thread 1: {}", i)).into_bytes())
            .build();
        channel.write().unwrap().send(msg);
        println!("Writing thread 2!")
    }

    let handle2 = thread::spawn(move || {
        for i in 1..100 {
            let routing = RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Fetch)
                .routing_type(RoutingType::Direct)
                .build();
            let msg = channel3.read().unwrap().get_factory()
                .routing_mod(routing)
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .build();
            if let Ok((_, data)) = channel3.write().unwrap().send_and_read(msg) {
                println!("Fetched from channel 2: {:?}", String::from_utf8(*data).unwrap().trim_end_matches("\0"));
            }
        }
    });

    for i in 1..100 {
        let routing = RoutingModFactory::new()
            .data_type(DataType::Message)
            .message_type(MessageType::Fetch)
            .routing_type(RoutingType::Direct)
            .build();
        let msg = channel.read().unwrap().get_factory()
            .routing_mod(routing)
            .route(Routing::Route(String::from("base_exc")))
            .route(Routing::Stop)
            .queue_name(String::from("base_queue"))
            .build();
        //channel.send(msg);
        if let Ok((_, data)) = channel.write().unwrap().send_and_read(msg) {
            println!("Fetched from channel 1: {:?}", String::from_utf8(*data).unwrap().trim_end_matches("\0"));
        }
    }

    handle.join().unwrap();
    handle2.join().unwrap();

    session.write().unwrap().close();
}


