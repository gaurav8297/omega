use std::time::Duration;
use std::net::SocketAddr;
use std::str::FromStr;

use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use crate::transport::Transport;

#[tokio::test]
async fn test_transport_write_to()
{
    let addr = get_random_local_addr();
    let mut transport = Transport::new(addr).await.unwrap();

    let data = b"tests data";
    transport.write_to(data, addr).await;

    let result = transport.packet_channel().recv().await.unwrap();
    assert_eq!(result.buf.as_slice(), data);

    transport.shutdown().await;
}

#[tokio::test]
async fn test_transport_connect_timeout()
{
    let addr1 = get_random_local_addr();
    let addr2 = get_random_local_addr();

    let transport = Transport::new(addr1).await.unwrap();
    let result = transport.connect_timeout(addr1, Duration::from_millis(100)).await;
    assert!(result.is_ok());

    let result = transport.connect_timeout(addr2, Duration::from_millis(100)).await;
    assert!(result.is_err());

    transport.shutdown().await;
}

#[tokio::test]
async fn test_transport_stream_channel()
{
    let addr = get_random_local_addr();
    let mut transport = Transport::new(addr).await.unwrap();

    let data = b"tests data";
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(data).await;

    let mut expected_data = [0; 10];
    let mut socket = transport.stream_channel().recv().await.unwrap();
    socket.read(&mut expected_data).await;
    assert_eq!(expected_data, *data);

    transport.shutdown().await;
}

fn get_random_local_addr() -> SocketAddr
{
    let port = portpicker::pick_unused_port().unwrap();
    let local_addr = format!("127.0.0.1:{}", port.to_string());
    return SocketAddr::from_str(local_addr.as_str()).unwrap();
}
