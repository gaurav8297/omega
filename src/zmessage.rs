use bytebuffer::ByteBuffer;
use uuid::Uuid;
use zmq::Socket;

/// Create beacon message following the ZRE format
///
/// # Message Format
///
/// Header: | Z | R | E | %x01 |
///
/// Body: | UUID | port |
pub fn create_beacon_message(port: u16) -> Vec<u8> {
    let mut res = ByteBuffer::new();

    // Write 'Z', 'R' and 'E'
    res.write_bytes(&convert_char_to_bytes('Z'));
    res.write_bytes(&convert_char_to_bytes('R'));
    res.write_bytes(&convert_char_to_bytes('E'));

    // Write version
    res.write_i8(1);

    // Uuid for a node
    let uuid = Uuid::new_v4();
    res.write_bytes(uuid.as_bytes());

    // port
    res.write_u16(port);
    return res.to_bytes();
}

fn convert_char_to_bytes(char: char) -> [u8; 1] {
    let mut res = [0; 1];
    char.encode_utf8(&mut res);
    return res;
}

///  These are the zre_msg messages
///     HELLO - Greet a peer so it can connect back to us
///         sequence      number 2  Cyclic sequence number
///         endpoint      string
///         groups        strings
///         status        number 1
///         name          string
///         headers       dictionary
///     WHISPER - Send a message to a peer
///         sequence      number 2
///         content       frame
///     SHOUT - Send a message to a group
///         sequence      number 2
///         group         string
///         content       frame
///     JOIN - Join a group
///         sequence      number 2
///         group         string
///         status        number 1
///     LEAVE - Leave a group
///         sequence      number 2
///         group         string
///         status        number 1
///     PING - Ping a peer that has gone silent
///         sequence      number 2
///     PING_OK - Reply to a peer's ping
///         sequence      number 2

pub fn send_hello_msg(
    output: Socket,
    sequence: u32,
    endpoint: String,
    groups: Vec<String>,
    status: bool,
    name: String,
    headers: Vec<String>)
{
    let mut res = ByteBuffer::new();
    res.write_u32(sequence);
    res.write_string(endpoint.as_str());

    // write groups
    res.write_u32(groups.len() as u32);
    groups.iter()
        .map(|group| res.write_string(group.as_str()));

    res.write_bit(status);
    res.write_string(name.as_str());

    // write header
    res.write_u32(headers.len() as u32);
    headers.iter()
        .map(|header| res.write_string(header.as_str()));

    send(output, res.to_bytes(), vec![], vec![]);
}

pub fn send_whisper(output: Socket, sequence: u32)
{
    let mut res = ByteBuffer::new();
    res.write_u32(sequence);

    send(output, res.to_bytes(), vec![], vec![]);
}

pub fn send_shout(output: Socket, sequence: u32, group: String)
{
    let mut res = ByteBuffer::new();
    res.write_u32(sequence);
    res.write_string(group.as_str());

    send(output, res.to_bytes(), vec![], vec![]);
}

pub fn send_join(output: Socket, sequence: u32, group: String, status: bool)
{
    let mut res = ByteBuffer::new();
    res.write_u32(sequence);
    res.write_string(group.as_str());
    res.write_bit(status);

    send(output, res.to_bytes(), vec![], vec![]);
}

pub fn send_ping(output: Socket, sequence: u32)
{
    let mut res = ByteBuffer::new();
    res.write_u32(sequence);

    send(output, res.to_bytes(), vec![], vec![]);
}

pub fn send_ping_ok(output: Socket, sequence: u32)
{
    let mut res = ByteBuffer::new();
    res.write_u32(sequence);

    send(output, res.to_bytes(), vec![], vec![]);
}

pub fn send(output: Socket, header: Vec<u8>, content: Vec<Vec<u8>>, node_id: Vec<u8>)
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
