use std::net::SocketAddr;
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::sync::Arc;

use log::error;
use socket2::{Socket, Domain, Type, Protocol, SockAddr};
use tokio::net::{TcpListener, UdpSocket, TcpStream};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio::time::timeout;
use tokio::task::JoinHandle;

const UDP_PACKET_BUFFER_SIZE: usize = 65536;
const UDP_RECEIVE_BUF_SIZE: usize = 2 * 1024 * 1024;
const UDP_READ_TIME_OUT: Duration = Duration::from_millis(30);
const TCP_LISTEN_TIME_OUT: Duration = Duration::from_millis(30);

pub struct Packet
{
    pub buf: Vec<u8>,
    pub from: SocketAddr,
    pub timestamp: Instant,
}

pub struct Address
{
    addr: SocketAddr,
    name: String,
}

pub struct Transport
{
    packets: UnboundedReceiver<Packet>,
    streams: UnboundedReceiver<TcpStream>,
    tcp_listener: Arc<TcpListener>,
    udp_socket: Arc<UdpSocket>,
    tcp_handle: JoinHandle<()>,
    udp_handle: JoinHandle<()>,
    is_shutdown: Arc<AtomicBool>,
}

impl Transport
{
    pub async fn new(bind_addr: SocketAddr) -> io::Result<Transport>
    {
        let tcp_listener = Arc::new(TcpListener::bind(bind_addr.clone()).await?);

        let std_socket = Socket::new(Domain::IPV4, Type::DGRAM, Option::Some(Protocol::UDP))?;
        std_socket.set_recv_buffer_size(UDP_RECEIVE_BUF_SIZE)?;
        std_socket.set_read_timeout(Option::Some(UDP_READ_TIME_OUT))?;
        std_socket.bind(&SockAddr::from(bind_addr.clone()))?;
        let udp_socket = Arc::new(UdpSocket::from_std(std_socket.into())?);

        let (stream_sender, stream_rec) = mpsc::unbounded_channel();
        let (packet_sender, packet_rec) = mpsc::unbounded_channel();

        let is_shutdown = Arc::new(AtomicBool::new(false));

        let tcp_handle = tokio::spawn(Self::start_tcp_listener(
            tcp_listener.clone(),
            stream_sender,
            is_shutdown.clone()));
        let udp_handle = tokio::spawn(Self::start_udp_listener(
            udp_socket.clone(),
            packet_sender,
            is_shutdown.clone()));

        let transport = Transport {
            packets: packet_rec,
            streams: stream_rec,
            tcp_listener: tcp_listener.clone(),
            udp_socket: udp_socket.clone(),
            tcp_handle,
            udp_handle,
            is_shutdown: is_shutdown.clone(),
        };
        return Ok(transport);
    }

    async fn start_udp_listener(udp_socket: Arc<UdpSocket>, sender: UnboundedSender<Packet>, is_shutdown: Arc<AtomicBool>)
    {
        loop {
            let mut buf = [0; UDP_PACKET_BUFFER_SIZE];
            if let Ok(result) = timeout(UDP_READ_TIME_OUT, udp_socket.recv_from(&mut buf)).await {
                match result
                {
                    Ok((size, addr)) => {
                        if size < 1 {
                            error!("No data to send");
                            continue;
                        }

                        sender.send(Packet {
                            buf: buf[0..size].to_vec(),
                            from: addr,
                            timestamp: Instant::now(),
                        });
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
            }

            if is_shutdown.load(Ordering::Acquire) {
                break;
            }
        }
    }

    async fn start_tcp_listener(tcp_listener: Arc<TcpListener>, sender: UnboundedSender<TcpStream>, is_shutdown: Arc<AtomicBool>)
    {
        loop {
            if let Ok(result) = timeout(TCP_LISTEN_TIME_OUT, tcp_listener.accept()).await {
                match result {
                    Ok((stream, _)) => {
                        sender.send(stream);
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
            }

            if is_shutdown.load(Ordering::Acquire) {
                break;
            }
        }
    }
}

impl Transport
{
    pub fn find_advertise_addr(&self) -> io::Result<SocketAddr>
    {
        return self.tcp_listener.local_addr();
    }

    pub async fn write_to(&self, data: &[u8], addr: SocketAddr) -> io::Result<Instant>
    {
        return match self.udp_socket.send_to(data, addr).await {
            Ok(_) => {
                Ok(Instant::now())
            }
            Err(e) => {
                Err(e)
            }
        };
    }

    pub fn packet_channel(&mut self) -> &mut UnboundedReceiver<Packet> {
        return &mut self.packets;
    }

    pub async fn connect_timeout(&self, addr: SocketAddr, duration: Duration) -> io::Result<TcpStream> {
        return match timeout(duration, TcpStream::connect(addr)).await {
            Ok(res) => {
                res
            }
            Err(e) => {
                Err(e.into())
            }
        };
    }

    pub fn stream_channel(&mut self) -> &mut UnboundedReceiver<TcpStream> {
        return &mut self.streams;
    }

    pub async fn shutdown(self) {
        self.is_shutdown.store(true, Ordering::Release);

        if let Err(e) = self.tcp_handle.await {
            error!("Error while stopping tcp listener: {}", e.to_string())
        }
        if let Err(e) = self.udp_handle.await {
            error!("Error while stopping udp socket: {}", e.to_string())
        }
    }
}
