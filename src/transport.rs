use std::net::{SocketAddr, AddrParseError, UdpSocket, Shutdown, TcpListener};
use std::time::{Instant, Duration};
use std::sync::mpsc::{Receiver, Sender};
use std::{thread, io};
use std::sync::mpsc;
use std::str::FromStr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::mem::MaybeUninit;
use std::io::Error;

use log::{info, error};
use socket2::{Socket, Domain, Type, Protocol, SockAddr};

const UDP_PACKET_BUFFER_SIZE: usize = 10;
const UDP_RECEIVE_BUF_SIZE: usize = 2 * 1024 * 1024;
const UDP_READ_TIME_OUT: Duration = Duration::from_millis(100);

static IS_TRANSPORT_SHUTDOWN: AtomicBool = AtomicBool::new(false);

pub struct Packet
{
    pub buf: Vec<u8>,
    pub from: SocketAddr,
    pub timestamp: Instant
}

pub struct Address
{
    addr: SocketAddr,
    name: String
}

pub trait Transport
{
    fn find_advertise_addr(&self) -> io::Result<SocketAddr>;
    fn write_to_address(&self, data: &[u8], addr: Address) -> io::Result<Instant>
    {
        return self.write_to(data, addr.addr);
    }
    fn write_to(&self, data: &[u8], addr: SocketAddr) -> io::Result<Instant>;
    fn packet_channel(&self) -> Rc<Receiver<Packet>>;
    fn connect_timeout_address(&self, addr: Address, timeout: Duration) -> io::Result<Socket>
    {
        return self.connect_timeout(addr.addr, timeout);
    }
    fn connect_timeout(&self, addr: SocketAddr, timeout: Duration) -> io::Result<Socket>;
    fn stream_channel(&self) -> Rc<Receiver<Socket>>;
    fn shutdown(self);
}

pub struct NetTransport
{
    packets: Rc<Receiver<Packet>>,
    streams: Rc<Receiver<Socket>>,
    tcp_listener: Socket,
    udp_socket: Socket,
    tcp_handle: Option<JoinHandle<()>>,
    udp_handle: Option<JoinHandle<()>>
}

impl NetTransport
{
    pub fn new(bind_addr: SocketAddr) -> io::Result<NetTransport>
    {
        let sock_addr = SockAddr::from(bind_addr);

        let tcp_socket = Socket::new(Domain::IPV4, Type::STREAM, Option::Some(Protocol::TCP))?;
        let bind_result = tcp_socket.bind(&sock_addr);
        tcp_socket.listen(128)?;
        tcp_socket.set_nonblocking(true);
        match bind_result {
            Ok(_) => {}
            Err(e) => {
                return Err(e)
            }
        }

        let udp_socket = Socket::new(Domain::IPV4, Type::DGRAM, Option::Some(Protocol::UDP))?;
        udp_socket.set_recv_buffer_size(UDP_RECEIVE_BUF_SIZE);
        udp_socket.set_read_timeout(Option::Some(UDP_READ_TIME_OUT));

        let bind_result = udp_socket.bind(&sock_addr);
        match bind_result {
            Ok(_) => {}
            Err(e) => {
                return Err(e)
            }
        }

        let (packet_sender, packet_rec): (Sender<Packet>, Receiver<Packet>) = mpsc::channel();
        let (stream_sender, stream_rec): (Sender<Socket>, Receiver<Socket>) = mpsc::channel();

        let mut transport = NetTransport {
            packets: Rc::new(packet_rec),
            streams: Rc::new(stream_rec),
            tcp_listener: tcp_socket,
            udp_socket,
            tcp_handle: None,
            udp_handle: None
        };

        transport.start_udp_listener_thread(packet_sender);
        transport.start_tcp_listener_thread(stream_sender);

        return Ok(transport);
    }

    fn start_udp_listener_thread(&mut self, sender: Sender<Packet>)
    {
        let socket = self.udp_socket.try_clone().unwrap();
        let udp_handle = thread::spawn(move || {
            loop {
                let mut buf = [MaybeUninit::<u8>::uninit(); UDP_PACKET_BUFFER_SIZE];
                let result = socket.recv_from(&mut buf);
                match result
                {
                    Ok((size, addr)) => {
                        if size < 1 {
                            error!("No data to send");
                            continue;
                        }

                        let mut data = vec![];
                        for i in 0..size {
                            unsafe { data.push(buf[i].assume_init()) }
                        }

                        sender.send(Packet {
                            buf: data,
                            from: addr.as_socket().unwrap(),
                            timestamp: Instant::now()
                        });
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }
                if IS_TRANSPORT_SHUTDOWN.load(Ordering::Acquire) {
                    break;
                }
            }
        });
        self.udp_handle = Option::Some(udp_handle);
    }

    fn start_tcp_listener_thread(&mut self, sender: Sender<Socket>)
    {
        let listener = self.tcp_listener.try_clone().unwrap();
        let tcp_handle = thread::spawn(move || {
            loop {
                let result_stream = listener.accept();
                match result_stream {
                    Ok((stream, _)) => {
                        sender.send(stream);
                    }
                    Err(e) => {
                        error!("{}", e.to_string());
                    }
                }

                if IS_TRANSPORT_SHUTDOWN.load(Ordering::Acquire) {
                    break;
                }

                info!("[Tcp Listen Thread] Sleeping for 30 ms..");
                thread::sleep(Duration::from_millis(30))
            }
        });
        self.tcp_handle = Option::Some(tcp_handle);
    }
}

impl Transport for NetTransport
{
    fn find_advertise_addr(&self) -> io::Result<SocketAddr>
    {
        return match self.tcp_listener.local_addr() {
            Ok(addr) => {
                Ok(addr.as_socket().unwrap())
            }
            Err(e) => {
                e
            }
        };
    }

    fn write_to(&self, data: &[u8], addr: SocketAddr) -> io::Result<Instant>
    {
        let result = self.udp_socket.send_to(data, &SockAddr::from(addr));
        return match result {
            Ok(_) => {
                Ok(Instant::now())
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn packet_channel(&self) -> Rc<Receiver<Packet>> {
        return self.packets.clone();
    }

    fn connect_timeout(&self, addr: SocketAddr, timeout: Duration) -> io::Result<Socket> {
        let tcp_socket = Socket::new(Domain::IPV4, Type::STREAM, Option::Some(Protocol::TCP))?;

        let result = tcp_socket.connect_timeout(&SockAddr::from(addr), timeout);
        return match result {
            Ok(_) => {
                Ok(tcp_socket)
            }
            Err(e) => {
                Err(e)
            }
        }
    }

    fn stream_channel(&self) -> Rc<Receiver<Socket>> {
        return self.streams.clone();
    }

    fn shutdown(self) {
        IS_TRANSPORT_SHUTDOWN.store(true, Ordering::Release);
        self.tcp_listener.shutdown(Shutdown::Both);

        if self.tcp_handle.is_some() {
            self.tcp_handle.unwrap().join();
        }

        if self.udp_handle.is_some() {
            self.udp_handle.unwrap().join();
        }
    }
}
