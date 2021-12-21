use omega::transport::{NetTransport, Transport};
use socket2::SockAddr;
use std::net::{SocketAddr, IpAddr, TcpStream, TcpListener};
use std::str::FromStr;
use std::thread;
use std::time::Duration;
use std::io::{Write, Read};

#[test]
fn test_transport_write_to()
{
    let addr = get_random_local_addr();
    let transport = NetTransport::new(addr).unwrap();

    let data = "test data".as_bytes();
    transport.write_to(data, addr);

    let result = transport.packet_channel().recv().unwrap();
    assert_eq!(result.buf.as_slice(), data);

    transport.shutdown();
}

#[test]
fn test_transport_connect_timeout()
{
    let addr1 = get_random_local_addr();
    let addr2 = get_random_local_addr();

    let transport = NetTransport::new(addr1).unwrap();
    let result = transport.connect_timeout(addr1, Duration::from_millis(100));
    assert!(result.is_ok());

    let result = transport.connect_timeout(addr2, Duration::from_millis(100));
    assert!(result.is_err());

    transport.shutdown();
}

#[test]
fn test_transport_stream_channel()
{
    let addr = get_random_local_addr();
    let transport = NetTransport::new(addr).unwrap();

    let data = "test data".as_bytes();
    let mut stream = TcpStream::connect(addr).unwrap();
    stream.write(data);

    let mut expected_data = [0; 9];
    let mut socket = transport.stream_channel().recv().unwrap();
    socket.read(&mut expected_data);
    assert_eq!(expected_data, data);

    transport.shutdown();
}

fn get_random_local_addr() -> SocketAddr
{
    let port = portpicker::pick_unused_port().unwrap();
    let local_addr = format!("127.0.0.1:{}", port.to_string());
    return SocketAddr::from_str(local_addr.as_str()).unwrap();
}
