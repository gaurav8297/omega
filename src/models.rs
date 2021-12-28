use std::net::SocketAddr;
use std::time::Instant;
use std::fmt::Debug;

use serde::{Serialize, Deserialize};
use rmp_serde::encode::Error;

/// Node models
#[derive(Clone, PartialEq)]
pub enum NodeStateKind {
    Alive,
    Suspect,
    Dead,
    Left
}

#[derive(Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub addr: SocketAddr,
    pub state: NodeStateKind
}

#[derive(Clone, PartialEq)]
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MessageKind {
    AliveMessage
}

/// Alive message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Alive {
    pub incarnation: u32,
    pub node: String,
    pub addr: SocketAddr
}

pub fn encode<T: ?Sized>(kind: MessageKind, message: &T) -> Result<Vec<u8>, Error>
    where
    T: Serialize
{
    let mut buf = rmp_serde::to_vec(&kind)?;
    let mut val = rmp_serde::to_vec(message)?;
    buf.append(&mut val);
    return Ok(buf);
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
