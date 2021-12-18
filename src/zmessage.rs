use bytebuffer::ByteBuffer;
use uuid::Uuid;
use zmq::Socket;
use crate::constant::ZRE_VERSION;
use crate::utils::convert_char_to_bytes;

/// Create beacon message following the ZRE format
///
/// # Message Format
///
/// Header: | Z | R | E | %x01 |
///
/// Body: | UUID | port |
pub fn send_beacon(node_id: Uuid, port: u16) -> Vec<u8>
{
    let mut res = ByteBuffer::new();

    // Write 'Z', 'R' and 'E'
    res.write_bytes(&convert_char_to_bytes('Z'));
    res.write_bytes(&convert_char_to_bytes('R'));
    res.write_bytes(&convert_char_to_bytes('E'));

    // Write version
    res.write_u8(ZRE_VERSION);

    // Uuid for a node
    res.write_bytes(node_id.as_bytes());

    // port
    res.write_u16(port);
    return res.to_bytes();
}

#[derive(Clone)]
pub enum ZMessageType
{
    Hello = 1,
    Whisper = 2,
    Shout = 3,
    Join = 4,
    Leave = 5,
    Ping = 6,
    PingOk = 7,
}

pub trait ZMessage
{
    fn send(&self, output: Socket, sequence: u16);
}

///     HELLO - Greet a peer so it can connect back to us
///         sequence      number 2  Cyclic sequence number
///         endpoint      string
///         groups        strings
///         status        number 1
///         name          string
///         headers       dictionary
pub struct HelloMessage
{
    endpoint: String,
    groups: Vec<String>,
    status: u8,
    name: String,
    headers: Vec<String>
}

impl HelloMessage
{
    fn new(endpoint: String,
           groups: Vec<String>,
           status: u8,
           name: String,
           headers: Vec<String>) -> HelloMessage
    {
        return HelloMessage {
            endpoint,
            groups,
            status,
            name,
            headers
        }
    }
}

impl ZMessage for HelloMessage
{
    fn send(&self, output: Socket, sequence: u16)
    {
        let mut res = create_base_msg(ZMessageType::Hello, sequence);
        res.write_string(endpoint.as_str());

        // write groups
        res.write_u32(self.groups.len() as u32);
        self.groups.iter()
            .for_each(|group| res.write_string(group.as_str()));

        res.write_u8(self.status);
        res.write_string(self.name.as_str());

        // write header
        res.write_u32(self.headers.len() as u32);
        self.headers.iter()
            .for_each(|header| res.write_string(header.as_str()));

        send(output, vec![], res.to_bytes(), vec![]);
    }
}

///     WHISPER - Send a message to a peer
///         sequence      number 2
///         content       frame
pub struct WhisperMessage
{
    content: Vec<Vec<u8>>
}

impl WhisperMessage
{
    fn new(content: Vec<Vec<u8>>) -> WhisperMessage
    {
        return WhisperMessage {
            content
        }
    }
}

impl ZMessage for WhisperMessage
{
    fn send(&self, output: Socket, sequence: u16) {
        let mut res = create_base_msg(ZMessageType::Whisper, sequence);
        send(output, vec![], res.to_bytes(), self.content.clone());
    }
}

///     SHOUT - Send a message to a group
///         sequence      number 2
///         group         string
///         content       frame
pub struct ShoutMessage
{
    group: String,
    content: Vec<Vec<u8>>
}

impl ShoutMessage {
    fn new(group: String, content: Vec<Vec<u8>>) -> ShoutMessage
    {
        return ShoutMessage {
            group,
            content
        }
    }
}

impl ZMessage for ShoutMessage
{
    fn send(&self, output: Socket, sequence: u16)
    {
        let mut res = create_base_msg(ZMessageType::Shout, sequence);
        res.write_string(group.as_str());

        send(output, res.to_bytes(), vec![], self.content.clone());
    }
}
///     JOIN - Join a group
///         sequence      number 2
///         group         string
///         status        number 1
pub struct MembershipMessage
{
    message_type: ZMessageType,
    group: String,
    status: u8
}

impl MembershipMessage
{
    fn new(message_type: ZMessageType, group: String, status: u8) -> MembershipMessage
    {
        if message_type != ZMessageType::Join || message_type != ZMessageType::Leave {
            panic!("Wrong message type: " + message_type)
        }
        return MembershipMessage {
            message_type,
            group,
            status
        };
    }
}

impl ZMessage for MembershipMessage
{
    fn send(&self, output: Socket, sequence: u16)
    {
        let mut res = create_base_msg(self.message_type.clone(), sequence);
        res.write_string(group.as_str());
        res.write_u8(status);

        send(output, res.to_bytes(), vec![], vec![]);
    }
}

///     PING - Ping a peer that has gone silent
///         sequence      number 2
pub struct PingMessage
{
    message_type: ZMessageType,
}

impl PingMessage
{
    fn new(message_type: ZMessageType) -> PingMessage
    {
        if message_type != ZMessageType::Ping || message_type != ZMessageType::PingOk {
            panic!("Wrong message type: " + message_type)
        }
        return PingMessage {
            message_type
        };
    }
}

impl ZMessage for PingMessage
{
    fn send(&self, output: Socket, sequence: u16)
    {
        let mut res = create_base_msg(self.message_type.clone(), sequence);
        send(output, res.to_bytes(), vec![], vec![]);
    }
}

pub fn create_base_msg(msg_type: ZMessageType, sequence: u16) -> ByteBuffer
{
    let mut res = ByteBuffer::new();
    res.write_u16(0xAAA0 | 1);
    res.write_u8(msg_type as u8);
    res.write_u8(ZRE_VERSION);
    res.write_u16(sequence);
    return res;
}

pub fn send(output: Socket, node_id: Vec<u8>, header: Vec<u8>, content: Vec<Vec<u8>>)
{
    if output.get_socket_type().unwrap() == zmq::ROUTER && !node_id.is_empty() {
        output.send(node_id, zmq::SNDMORE)
            .expect("Some error!");
    }

    if content.is_empty() {
        output.send(header, zmq::DONTWAIT)
            .expect("Some error!")
    }
    else {
        output.send(header, zmq::SNDMORE)
            .expect("Some error!");
        output.send_multipart(content, zmq::DONTWAIT)
            .expect("Some error!");
    }
}
