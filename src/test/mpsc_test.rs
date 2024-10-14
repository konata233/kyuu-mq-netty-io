use std::sync::{Arc, RwLock};
use std::thread;
use crate::mq::io::factory::{CommandType, DataType, MessageType, Routing, RoutingModFactory, RoutingType};
use crate::mq::io::session;

pub fn mpsc_test() -> Result<(), Box<dyn std::error::Error>> {
    let mut session = session::Session::new("127.0.0.1:11451", "MQ_HOST".to_string());
    let mut session = Arc::from(RwLock::from(session));
    session
        .clone()
        .write()
        .unwrap()
        .init(session.clone());

    println!("Conn established!");
    session.write().unwrap().set_read_timeout(std::time::Duration::from_millis(500));
    let channel = session.write().unwrap().create_channel("MQ_CHANNEL".to_string()).clone().unwrap();
    let channel2 = session.write().unwrap().create_channel("MQ_CHANNEL2".to_string()).clone().unwrap();
    let channel2_cloned = channel2.clone();
    let channel_cloned = channel.clone();
    let channel_r  = session.write().unwrap().create_channel("MQ_CHANNEL_R".to_string()).clone().unwrap();

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
        let mut i = 0;
        loop {
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
                .data(String::from(format!("hello world from thread 2: {i}")).into_bytes())
                .build();
            channel2.write().unwrap().send(msg);
            println!("Writing thread 2: {i}!");
            i += 1;
            thread::sleep(std::time::Duration::from_millis(100));
        }
    });

    let handle3 = thread::spawn(move || {
        let mut i = 0;
        loop {
            let msg = channel_cloned.read().unwrap().get_factory()
                .routing_mod(RoutingModFactory::new()
                    .data_type(DataType::Message)
                    .message_type(MessageType::Push)
                    .routing_type(RoutingType::Direct)
                    .build()
                )
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .data(String::from(format!("hello world from thread 3: {}", i)).into_bytes())
                .build();
            channel_cloned.write().unwrap().send(msg);
            println!("Writing thread 3: {i}!");
            i += 1;
            thread::sleep(std::time::Duration::from_millis(300));
        }
    });


    let handle2 = thread::spawn(move || {
        loop {
            let routing = RoutingModFactory::new()
                .data_type(DataType::Message)
                .message_type(MessageType::Fetch)
                .routing_type(RoutingType::Direct)
                .build();
            let msg = channel_r.read().unwrap().get_factory()
                .routing_mod(routing)
                .route(Routing::Route(String::from("base_exc")))
                .route(Routing::Stop)
                .queue_name(String::from("base_queue"))
                .build();
            if let Ok((_, data)) = channel_r.write().unwrap().send_and_read(msg) {
                println!("Fetched from channel read: {:?}", String::from_utf8(*data).unwrap().trim_end_matches("\0"));
            } else {
                println!("Failed to fetch from channel read!");
            }
            thread::sleep(std::time::Duration::from_millis(200));
        }
    });

    thread::scope(|s| {
        s.spawn(move || handle3.join().unwrap());
        s.spawn(move || handle.join().unwrap());
        s.spawn(move || handle2.join().unwrap());
    });
    channel.write().unwrap().close();
    channel2_cloned.write().unwrap().close();
    session.write().unwrap().close();
    Ok(())
}