use std::net::SocketAddr;
use std::time::Instant;
use bincode::{Decode, Encode, ErrorKind};

/// Node models
pub enum NodeStateKind {
    Alive,
    Suspect,
    Dead,
    Left
}

pub struct Node {
    pub name: String,
    pub addr: SocketAddr,
    pub state: NodeStateKind
}

#[derive(Clone)]
pub struct NodeState {
    pub node: Node,
    pub state: NodeStateKind,
    pub incarnation: u32,
    pub state_change: Instant
}

impl NodeState {
    pub fn get_name(&self) -> String
    {
        return self.node.name.clone();
    }

    pub fn get_addr(&self) -> SocketAddr
    {
        return self.node.addr.clone();
    }
}

/// Messages
pub enum MessageKind {
    AliveMessage
}

/// Alive message
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Alive {
    pub incarnation: u32,
    pub node: String,
    pub addr: SocketAddr
}

pub fn encode<T: ?Sized>(kind: MessageKind, message: &T) -> bincode::Result<Vec<u8>>
where
    T: serde::Serialize,
{
    let mut buff = bincode::serialize(&kind).unwrap();
    return match bincode::serialize(message) {
        Ok(mut val) => {
            buff.append(&mut val);
            Ok(buff)
        }
        Err(e) => {
            e
        }
    }
}
//
// pub fn
//
// pub fn decode<'a, T>(bytes: &'a [u8]) -> bincode::Result<T>
//     where
//         T: serde::de::Deserialize<'a>,
// {
//     let message_kind: bincode::Result<MessageKind> = bincode::deserialize(bytes.slice(0, 1));
//     match message_kind {
//         Ok(kind) => {
//             match kind {
//                 MessageKind::AliveMessage => {
//                     bincode::deserialize();
//                 }
//             }
//         }
//         Err(e) => {
//             e
//         }
//     }
// }
