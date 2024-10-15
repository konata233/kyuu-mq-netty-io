use crate::mq::protocol::protobase::{Deserialize, Serialize};

#[derive(Debug)]
pub struct DataHead {
    pub virtual_host: [u8; 32],

    pub channel: [u8; 32],

    pub version: [u8; 4],
    pub routing_mod: [u8; 4],
    pub command: [u8; 24],

    pub route0: [u8; 32],

    pub route1: [u8; 32],

    pub route2: [u8; 32], // '*' to match any; '!' to pause

    pub route3: [u8; 32], // used as queue name

    pub slice_count: u32,
    pub slice_size: u32, // 512
    pub count: u32,
    pub errcode: u16,
    pub ack: u16,
    pub reserved: [u8; 16]
}

impl DataHead {
    pub fn new(virtual_host: String,
               channel: [u8; 32],
               routing_mod: [u8; 4],
               command: [u8; 24],
               route: [u8; 128],
               slice_count: u32,
               slice_size: u32,
               count: u32,
               msg_sign: u16) -> DataHead {
        let mut host = virtual_host.as_bytes().to_vec();
        host.resize(32, 0u8);
        DataHead {
            virtual_host: <[u8; 32]>::try_from(host).unwrap(),
            channel,
            version: [1u8, 0u8, 0u8, 0u8],
            routing_mod,
            command,
            route0: <[u8; 32]>::try_from(&route[0..32]).unwrap(),
            route1: <[u8; 32]>::try_from(&route[32..64]).unwrap(),
            route2: <[u8; 32]>::try_from(&route[64..96]).unwrap(),
            route3: <[u8; 32]>::try_from(&route[96..128]).unwrap(),
            slice_count,
            slice_size,
            count,
            errcode: msg_sign,
            ack: 0,
            reserved: [0u8; 16]
        }
    }
}

impl Serialize<256> for DataHead {
    fn serialize(&self) -> [u8; 256] {
        let serialized = self.serialize_vec();
        <[u8; 256]>::try_from(serialized).unwrap()
    }

    fn serialize_vec(&self) -> Vec<u8> {
        let mut serialized = vec![];
        serialized.append(&mut self.virtual_host.to_vec());
        serialized.append(&mut self.channel.to_vec());
        serialized.append(&mut self.version.to_vec());
        serialized.append(&mut self.routing_mod.to_vec());
        serialized.append(&mut self.command.to_vec());
        serialized.append(&mut self.route0.to_vec());
        serialized.append(&mut self.route1.to_vec());
        serialized.append(&mut self.route2.to_vec());
        serialized.append(&mut self.route3.to_vec());
        serialized.append(&mut self.slice_count.to_le_bytes().to_vec());
        serialized.append(&mut self.slice_size.to_le_bytes().to_vec());
        serialized.append(&mut self.count.to_le_bytes().to_vec());
        serialized.append(&mut self.errcode.to_le_bytes().to_vec());
        serialized.append(&mut self.ack.to_le_bytes().to_vec());
        serialized.append(&mut self.reserved.to_vec());
        serialized
    }
}

impl Deserialize<256> for DataHead {
    type T = DataHead;

    fn deserialize(data: [u8; 256]) -> Self::T {
        let mut deserialized = data.to_vec();
        let virtual_host_sha256 = <[u8; 32]>::try_from(deserialized.drain(0..32).collect::<Vec<_>>()).unwrap();

        let channel_sha256 = <[u8; 32]>::try_from(deserialized.drain(0..32).collect::<Vec<_>>()).unwrap();
        let version = <[u8; 4]>::try_from(deserialized.drain(0..4).collect::<Vec<_>>()).unwrap();
        let routing_mod = <[u8; 4]>::try_from(deserialized.drain(0..4).collect::<Vec<_>>()).unwrap();
        let command = <[u8; 24]>::try_from(deserialized.drain(0..24).collect::<Vec<_>>()).unwrap();

        let route0 = <[u8; 32]>::try_from(deserialized.drain(0..32).collect::<Vec<_>>()).unwrap();

        let route1 = <[u8; 32]>::try_from(deserialized.drain(0..32).collect::<Vec<_>>()).unwrap();

        let route2 = <[u8; 32]>::try_from(deserialized.drain(0..32).collect::<Vec<_>>()).unwrap();

        let route3 = <[u8; 32]>::try_from(deserialized.drain(0..32).collect::<Vec<_>>()).unwrap();

        let slice_count = <u32>::from_le_bytes(<[u8; 4]>::try_from(deserialized.drain(0..4).collect::<Vec<_>>()).unwrap());
        let slice_size = <u32>::from_le_bytes(<[u8; 4]>::try_from(deserialized.drain(0..4).collect::<Vec<_>>()).unwrap());
        let count = <u32>::from_le_bytes(<[u8; 4]>::try_from(deserialized.drain(0..4).collect::<Vec<_>>()).unwrap());
        let msg_sign = <u16>::from_le_bytes(<[u8; 2]>::try_from(deserialized.drain(0..2).collect::<Vec<_>>()).unwrap());
        let ack = <u16>::from_le_bytes(<[u8; 2]>::try_from(deserialized.drain(0..2).collect::<Vec<_>>()).unwrap());
        let reserved = <[u8; 16]>::try_from(deserialized.drain(0..16).collect::<Vec<_>>()).unwrap();

        DataHead {
            virtual_host: virtual_host_sha256,
            channel: channel_sha256,
            version,
            routing_mod,
            command,
            route0,
            route1,
            route2,
            route3,
            slice_count,
            slice_size,
            count,
            errcode: msg_sign,
            ack,
            reserved
        }
    }
}