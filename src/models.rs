use std::fmt::Debug;
use std::net::SocketAddr;
use std::time::SystemTime;

use rmp_serde::encode::Error;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Node models
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum NodeStateKind {
    Alive,
    Suspect,
    Dead,
    Left,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub addr: SocketAddr,
    pub state: NodeStateKind,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NodeState {
    pub node: Node,
    pub state: NodeStateKind,
    pub incarnation: u32,
    pub state_change: SystemTime,
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

    pub fn dead_or_left(&self) -> bool
    {
        return self.state == NodeStateKind::Dead || self.state == NodeStateKind::Left;
    }
}

/// Messages
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum MessageKind
{
    AliveMsg = 0,
    PingMsg = 1,
    PushPullMsg = 2,
    DeadMsg = 3,
    SuspectMsg = 4,
    UnknownMsg,
}

impl From<u8> for MessageKind
{
    #[inline]
    fn from(val: u8) -> Self
    {
        return match val {
            0 => MessageKind::AliveMsg,
            1 => MessageKind::PingMsg,
            2 => MessageKind::PushPullMsg,
            _ => MessageKind::UnknownMsg
        };
    }
}

/// Alive message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct AliveMessage {
    pub incarnation: u32,
    pub node: String,
    pub addr: SocketAddr,
}

/// PushPull Message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PushPullMessage {
    pub node_states: Vec<NodeState>,
    pub join: bool,
}

/// Dead Message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct DeadMessage {
    pub incarnation: u32,
    pub node: String,
    pub from: String,
}

pub async fn encode_to_tcp_stream<T: ?Sized>(kind: MessageKind, message: &T, stream: &mut TcpStream) -> io::Result<usize>
    where
        T: Serialize
{
    let out = encode(kind, message).unwrap();
    return stream.write(out.as_slice()).await;
}

pub fn encode<T: ?Sized>(kind: MessageKind, message: &T) -> Result<Vec<u8>, Error>
    where
        T: Serialize
{
    let mut buf = vec![kind as u8];
    let mut ser_message = rmp_serde::to_vec(message)?;
    buf.push(ser_message.len() as u8);
    buf.append(&mut ser_message);
    return Ok(buf);
}

pub async fn read_message_header(stream: &mut TcpStream) -> io::Result<(MessageKind, usize)>
{
    let mut buff = [0; 2];
    let _ = stream.read(&mut buff).await?;
    return Ok((buff[0].into(), buff[1] as usize));
}

pub async fn decode_from_tcp_stream<T: ?Sized>(size: usize, stream: &mut TcpStream) -> io::Result<T>
    where
        T: DeserializeOwned
{
    let mut buff = vec![0; size];
    let _ = stream.read(buff.as_mut_slice()).await?;

    return match rmp_serde::from_read_ref(&buff) {
        Ok(node_states) => {
            Ok(node_states)
        }
        Err(_) => {
            panic!()
        }
    };
}
