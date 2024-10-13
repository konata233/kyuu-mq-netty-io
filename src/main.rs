pub mod mq;

use std::io::{Read, Write};
use crate::mq::io::factory::{CommandType, DataType, MessageType, Routing, RoutingMod, RoutingModFactory, RoutingType};
use crate::mq::io::session;
use crate::mq::protocol::protobase::Serialize;
use crate::mq::protocol::protobase::Deserialize;

fn main() {
    let mut session = session::Session::new("127.0.0.1:11451", "MQ_HOST".to_string());
    println!("Conn established!");
    session.set_read_timeout(std::time::Duration::from_secs(5));
    let channel = session.create_channel("MQ_CHANNEL".to_string()).unwrap();

    let msg = channel.get_factory()
        .routing_mod(RoutingModFactory::new()
            .data_type(DataType::Command)
            .command_type(CommandType::NewExchange)
            .routing_type(RoutingType::Direct)
            .build()
        )
        .route(Routing::Stop)
        .data(String::from("base_exc").into_bytes())
        .build();
    channel.send(msg);

    let msg = channel.get_factory()
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
    channel.send(msg);

    for i in 1..100 {
        let msg = channel.get_factory()
            .routing_mod(RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Push)
                .routing_type(RoutingType::Direct)
                .build()
            )
            .route(Routing::Route(String::from("base_exc")))
            .route(Routing::Stop)
            .queue_name(String::from("base_queue"))
            .data(String::from(format!("hello world {}", i)).into_bytes())
            .build();
        channel.send(msg);
    }

    for i in 1..100 {
        let routing = RoutingModFactory::new()
            .data_type(DataType::Message)
            .message_type(MessageType::Fetch)
            .routing_type(RoutingType::Direct)
            .build();
        let msg = channel.get_factory()
            .routing_mod(routing)
            .route(Routing::Route(String::from("base_exc")))
            .route(Routing::Stop)
            .queue_name(String::from("base_queue"))
            .build();
        //channel.send(msg);
        let (_, data) = channel.send_and_read(msg);
        println!("Fetched: {:?}", String::from_utf8(data).unwrap().trim_end_matches("\0"));
    }
    channel.close();
    session.close();
}


