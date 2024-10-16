use std::sync::{Arc, RwLock};
use std::thread;
use crate::mq::io::factory::{CommandType, DataType, MessageType, Routing, RoutingModFactory, RoutingType};
use crate::mq::io::session;

pub fn spmc_test() -> Result<(), Box<dyn std::error::Error>> {
    let session = session::Session::new("127.0.0.1:11451", "MQ_HOST".to_string());
    let session = Arc::from(RwLock::from(session));
    session
        .clone()
        .write()
        .unwrap()
        .init(session.clone());

    session.write().unwrap().set_read_timeout(std::time::Duration::from_millis(300));

    let producer = session.write().unwrap().create_channel("MQ_CHANNEL_P".to_string()).unwrap();
    let consumer1 = session.write().unwrap().create_channel("MQ_CHANNEL_C1".to_string()).unwrap();
    let consumer2 = session.write().unwrap().create_channel("MQ_CHANNEL_C2".to_string()).unwrap();

    let msg = producer.read().unwrap().get_factory()
        .routing_mod(RoutingModFactory::new()
            .data_type(DataType::Command)
            .command_type(CommandType::NewExchange)
            .routing_type(RoutingType::Direct)
            .build()
        )
        .route(Routing::Stop)
        .data(String::from("base_exc").into_bytes())
        .build();
    producer.write().unwrap().send(msg);

    let msg = producer.read().unwrap().get_factory()
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
    producer.write().unwrap().send(msg);

    let handle_producer = thread::spawn(move || {
        for i in 1..10 {
            let routing = RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Push)
                .routing_type(RoutingType::Direct)
                .build();
            let msg = producer.read().unwrap().get_factory()
                .routing_mod(routing)
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .data(String::from(format!("hello world from producer: {i}")).into_bytes())
                .build();
            producer.write().unwrap().send(msg);
            thread::sleep(std::time::Duration::from_millis(50));
            println!("Sent from producer: {i}");
        }
        producer.write().unwrap().close();
    });

    let handle_consumer1 = thread::spawn(move || {
        for _ in 1..4 {
            let routing = RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Fetch)
                .routing_type(RoutingType::Direct)
                .build();
            let msg = consumer1.read().unwrap().get_factory()
                .routing_mod(routing)
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .build();
            if let Ok((head, data)) = consumer1.write().unwrap().send_and_read(msg) {
                if head.unwrap().errcode == 0xfu16 {
                    println!("No msg.");
                    thread::sleep(std::time::Duration::from_millis(60));
                } else {
                    println!("Fetched from channel consumer 1: {:?}", String::from_utf8(*data).unwrap().trim_end_matches("\0"));
                }
            } else {
                println!("Failed to fetch from consumer 1!");
            }
        }
        consumer1.write().unwrap().close();
    });

    let handle_consumer2 = thread::spawn(move || {
        for _ in 1..4 {
            let routing = RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Fetch)
                .routing_type(RoutingType::Direct)
                .build();
            let msg = consumer2.read().unwrap().get_factory()
                .routing_mod(routing)
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .build();
            if let Ok((head, data)) = consumer2.write().unwrap().send_and_read(msg) {
                if head.unwrap().errcode == 0xfu16 {
                    println!("No msg.");
                    thread::sleep(std::time::Duration::from_millis(60));
                } else {
                    println!("Fetched from channel consumer 2: {:?}", String::from_utf8(*data).unwrap().trim_end_matches("\0"));
                }
            } else {
                println!("Failed to fetch from consumer 2!");
            }
            thread::sleep(std::time::Duration::from_millis(60));
        }
        consumer2.write().unwrap().close();
    });

    thread::scope(|s| {
        s.spawn(move || handle_producer.join().unwrap());
        s.spawn(move || handle_consumer1.join().unwrap());
        s.spawn(move || handle_consumer2.join().unwrap());
    });

    session.write().unwrap().close();
    Ok(())
}