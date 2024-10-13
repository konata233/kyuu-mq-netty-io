#[derive(Debug)]
pub enum RoutingKey {
    Direct([String; 4]),
    Topic([String; 4]),
    Fanout([String; 4]),
}

impl RoutingKey {
    pub(crate) fn clone(&self) -> RoutingKey {
        match self {
            RoutingKey::Direct(arr) => RoutingKey::Direct(arr.clone()),
            RoutingKey::Topic(arr) => RoutingKey::Topic(arr.clone()),
            RoutingKey::Fanout(arr) => RoutingKey::Fanout(arr.clone()),
        }
    }
}