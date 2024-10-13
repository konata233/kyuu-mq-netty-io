use std::cmp::min;
use crate::mq::protocol::proto::DataHead;
use crate::mq::protocol::protobase::Serialize;

pub enum Command {
    CloseChannel,
}

#[repr(u8)]
#[derive(Debug)]
pub enum DataType {
    Message = 0u8,
    Command = 1u8
}

#[repr(u8)]
#[derive(Debug)]
pub enum CommandType {
    NewQueue = 0u8,
    NewExchange = 1u8,
    NewBinding = 2u8,

    DropQueue = 3u8,
    DropExchange = 4u8,
    DropBinding = 5u8,

    Nop = 0xfu8
}

#[repr(u8)]
#[derive(Debug)]
pub enum MessageType {
    Push = 0u8,
    Fetch = 1u8,
    Nop = 0xfu8
}

#[repr(u8)]
#[derive(Debug)]
pub enum RoutingType {
    Direct = 0u8,
    Topic = 1u8,
    Fanout = 2u8,
    Nop = 0xfu8
}

#[derive(Debug)]
pub struct RoutingMod {
    pub data_type: DataType,
    pub command_type: Option<CommandType>,
    pub message_type: Option<MessageType>,
    pub routing_type: RoutingType
}

#[derive(Debug)]
pub enum Routing {
    Route(String),
    Any,
    Stop,
}

pub struct MessageFactory {
    host: String,
    channel: String,
    version: [u8; 4],
    routing_mod: Option<RoutingMod>,
    command: Option<Command>,
    route: Vec<Routing>,
    queue_name: String,
    data: Vec<u8>
}

pub struct RoutingModFactory {
    data_type: DataType,
    command_type: Option<CommandType>,
    message_type: Option<MessageType>,
    routing_type: RoutingType
}

impl RoutingModFactory {
    pub fn new() -> RoutingModFactory {
        RoutingModFactory {
            data_type: DataType::Message,
            command_type: Some(CommandType::Nop),
            message_type: Some(MessageType::Nop),
            routing_type: RoutingType::Direct
        }
    }

    pub fn data_type(mut self, data_type: DataType) -> RoutingModFactory {
        self.data_type = data_type;
        self
    }

    pub fn command_type(mut self, command_type: CommandType) -> RoutingModFactory {
        self.command_type = Some(command_type);
        self
    }

    pub fn message_type(mut self, message_type: MessageType) -> RoutingModFactory {
        self.message_type = Some(message_type);
        self
    }

    pub fn routing_type(mut self, routing_type: RoutingType) -> RoutingModFactory {
        self.routing_type = routing_type;
        self
    }

    pub fn build(self) -> RoutingMod {
        RoutingMod {
            data_type: self.data_type,
            command_type: self.command_type,
            message_type: self.message_type,
            routing_type: self.routing_type
        }
    }
}

impl MessageFactory {
    pub fn new(host: String, channel: String) -> MessageFactory {
        MessageFactory {
            host,
            channel,
            version: [1u8, 0u8, 0u8, 0u8],
            routing_mod: Some(
                RoutingMod {
                    data_type: DataType::Message,
                    command_type: Some(CommandType::Nop),
                    message_type: Some(MessageType::Nop),
                    routing_type: RoutingType::Direct
                }
            ),
            command: None,
            route: vec![],
            queue_name: String::from(""),
            data: vec![]
        }
    }

    pub fn routing_mod(mut self, routing_mod: RoutingMod) -> MessageFactory {
        self.routing_mod = Some(routing_mod);
        self
    }

    pub fn command(mut self, command: Command) -> MessageFactory {
        self.command = Some(command);
        self
    }

    pub fn route(mut self, route: Routing) -> MessageFactory {
        self.route.push(route);
        self
    }

    pub fn queue_name(mut self, queue_name: String) -> MessageFactory {
        self.queue_name = queue_name;
        self
    }

    pub fn data(mut self, mut data: Vec<u8>) -> MessageFactory {
        if data.len() % 256 != 0 {
            let delta = 256 - data.len() % 256;
            data.resize(data.len() + delta, 0u8);
        }
        self.data = data;
        self
    }

    pub fn build(mut self) -> Vec<u8> {
        let mut serialized = vec![];
        let mut channel_serialized = self.channel.as_bytes().to_vec();
        channel_serialized.resize(32, 0u8);

        let mut routing_mod: [u8; 4] = [0u8; 4];
        let mut routing = self.routing_mod.unwrap();
        match routing.data_type {
            DataType::Message => {
                routing_mod[0] = 0u8;
                match routing.message_type.unwrap() {
                    MessageType::Push => {
                        routing_mod[1] = 0u8;
                    }
                    MessageType::Fetch => {
                        routing_mod[1] = 1u8;
                    }
                    MessageType::Nop => {
                        routing_mod[1] = 0xfu8;
                    }
                }
            }
            DataType::Command => {
                routing_mod[0] = 1u8;
                match routing.command_type.unwrap() {
                    CommandType::NewQueue => {
                        routing_mod[1] = 0u8;
                    }
                    CommandType::NewExchange => {
                        routing_mod[1] = 1u8;
                    }
                    CommandType::NewBinding => {
                        routing_mod[1] = 2u8;
                    }
                    CommandType::DropQueue => {
                        routing_mod[1] = 3u8;
                    }
                    CommandType::DropExchange => {
                        routing_mod[1] = 4u8;
                    }
                    CommandType::DropBinding => {
                        routing_mod[1] = 5u8;
                    }
                    CommandType::Nop => {
                        routing_mod[1] = 0xfu8;
                    }
                }
            }
        }
        match routing.routing_type {
            RoutingType::Direct => {
                routing_mod[2] = 0u8;
            }
            RoutingType::Topic => {
                routing_mod[2] = 1u8;
            }
            RoutingType::Fanout => {
                routing_mod[2] = 2u8;
            }
            RoutingType::Nop => {
                routing_mod[2] = 0xfu8;
            }
        }

        let command_serialized: [u8; 24] =
            if let Some(command) = self.command {
                match command {
                    Command::CloseChannel => {
                        let mut vec = String::from("CLOSE-CH").as_bytes().to_vec();
                        vec.resize(24, 0u8);
                        <[u8; 24]>::try_from(vec).unwrap()
                    }
                }
            } else {
                [0u8; 24]
            };

        // dbg!(&self.route);
        let route = self.route;
        let route_serialized: [u8; 128];
        let mut route_tmp: [[u8; 32]; 3] = [[0u8; 32]; 3];
        for i in 0..min(route.len(), 3) {
            match route[i] {
                Routing::Route(ref s) => {
                    let mut vec = s.as_bytes().to_vec();
                    vec.resize(32, 0u8);
                    route_tmp[i] = <[u8; 32]>::try_from(vec).unwrap();
                }
                Routing::Stop => {
                    let mut vec = String::from("!").as_bytes().to_vec();
                    vec.resize(32, 0u8);
                    route_tmp[i] = <[u8; 32]>::try_from(vec).unwrap();
                }
                Routing::Any => {
                    panic!("Not implemented yet.")
                }
            }
        }
        // dbg!(&route_tmp);

        // if the routing keys are not completely filled...
        for i in 0..(3 - route.len()) {
            let mut data = String::from("\0").into_bytes().to_vec();
            data.resize(32, 0u8);
            route_tmp[2 - i] = <[u8; 32]>::try_from(data).unwrap();
        }

        let mut queue_tmp = self.queue_name.into_bytes();
        queue_tmp.resize(32, 0u8);
        route_serialized = <[u8; 128]>::try_from([
            route_tmp[0],
            route_tmp[1],
            route_tmp[2],
            <[u8; 32]>::try_from(queue_tmp).unwrap()
        ].concat()).unwrap();

        let mut head = DataHead::new(
            self.host,
            <[u8; 32]>::try_from(channel_serialized).unwrap(),
            routing_mod,
            command_serialized,
            route_serialized,
            1,
            self.data.len() as u32,
            0u32,
            0u16
        );

        serialized.append(&mut head.serialize_vec());
        serialized.append(&mut self.data);
        serialized
    }
}