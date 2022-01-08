use std::net::SocketAddr;
use std::time::Instant;
use std::fmt::Debug;

use serde::{Serialize, Deserialize};
use rmp_serde::encode::Error;
use tokio::io;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Node models
#[derive(Clone, PartialEq)]
pub enum NodeStateKind {
    Alive,
    Suspect,
    Dead,
    Left,
}

#[derive(Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub addr: SocketAddr,
    pub state: NodeStateKind,
}

#[derive(Clone, PartialEq)]
pub struct NodeState {
    pub node: Node,
    pub state: NodeStateKind,
    pub incarnation: u32,
    pub state_change: Instant,
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
pub enum MessageKind
{
    AliveMessage = 0,
    PingMessage = 1,
    PushPullMessage = 2,
    DeadMessage = 3,
    SuspectMessage = 4,
    Unknown
}

impl From<u8> for MessageKind
{
    #[inline]
    fn from(val: u8) -> Self
    {
        return match val {
            0 => MessageKind::AliveMessage,
            1 => MessageKind::PingMessage,
            2 => MessageKind::PushPullMessage,
            _ => MessageKind::Unknown
        };
    }
}

/// Alive message
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Alive {
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
    pub from: String
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
    let buff = [0; 2];
    let _ = stream.read(&buff).await?;
    return Ok((buff[0].into(), buff[1] as usize));
}

pub async fn decode_from_tcp_stream<T: ?Sized>(size: usize, stream: &mut TcpStream) -> io::Result<T>
where
    T: Deserialize
{
    let mut buff = vec![0; size];
    let _ = stream.read(buff.as_mut_slice()).await?;

    return match rmp_serde::from_read(buff) {
        Ok(node_states) => {
            Ok(node_states)
        }
        Err(e) => {
            panic!()
        }
    }
}
