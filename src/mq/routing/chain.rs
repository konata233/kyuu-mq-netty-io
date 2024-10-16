use crate::mq::io::factory::Routing;

#[derive(Clone)]
pub struct RoutingChain {
    pub routing_key: [Routing; 3],
    pub queue_name: String,
}

impl RoutingChain {
    pub fn new(routing_key: [Routing; 3], queue_name: String) -> Self {
        RoutingChain {
            routing_key,
            queue_name,
        }
    }
}

pub struct RoutingChainFactory {
    pub routing_keys: Vec<Routing>,
    pub queue_name: Option<String>,
}

impl RoutingChainFactory {
    pub fn new() -> Self {
        RoutingChainFactory {
            routing_keys: Vec::new(),
            queue_name: None,
        }
    }

    pub fn add_key(mut self, routing_key: Routing) -> Self {
        self.routing_keys.push(routing_key);
        self
    }

    pub fn set_queue_name(mut self, queue_name: String) -> Self {
        self.queue_name = Some(queue_name);
        self
    }

    pub fn build(self) -> RoutingChain {
        let queue_name = self.queue_name.unwrap();
        let mut routing_key = [const { Routing::Stop }; 3];
        for (i, key) in self.routing_keys.iter().cloned().enumerate() {
            if i > 2 {
                break;
            }
            routing_key[i] = key;
        }
        RoutingChain::new(routing_key, queue_name)
    }
}