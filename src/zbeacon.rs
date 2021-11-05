use crate::constant::TERMINATE_COMMAND;
use crate::utils::{convert_bytes_to_string, convert_uninit_buffer_to_vec, check_if_vec_equal};
use crate::zactor::ZHandler;
use log::{debug, warn, info, error};
use pnet::ipnetwork::{IpNetwork, Ipv4Network};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4};
use std::ops::Sub;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use std::sync::Arc;
use std::time::{Duration, Instant};
use zmq::{Context, PollItem, POLLIN};
use std::mem::MaybeUninit;
use std::io::Error;

const CONFIGURE_COMMAND: &str = "CONFIGURE";
const PUBLISH_COMMAND: &str = "PUBLISH";
const SILENCE_COMMAND: &str = "SILENCE";
const SUBSCRIBE_COMMAND: &str = "SUBSCRIBE";
const UNSUBSCRIBE_COMMAND: &str = "UNSUBSCRIBE";
const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
const ENET_DOWN: i32 = 50;
const ENET_UNREACHED: i32 = 51;

pub struct ZBeacon {
    socket: Socket,
    network: IpNetwork,
    port: u16,
    is_terminated: bool,
    transmit: Option<Vec<u8>>,
    filter: Option<Vec<u8>>,
    apply_filter: bool,
    interval: Duration,
    ping_at: Instant,
}

impl ZBeacon {
    #[inline]
    pub fn new(port: u16) -> ZBeacon {
        let network = ZBeacon::find_available_network();
        let socket = ZBeacon::prepare_udp_socket(network, port);

        return ZBeacon {
            socket,
            network,
            port,
            is_terminated: false,
            transmit: Option::None,
            filter: Option::None,
            apply_filter: false,
            interval: DEFAULT_INTERVAL,
            ping_at: Instant::now(),
        };
    }

    pub fn prepare_udp_socket(ip_network: IpNetwork, socket_port: u16) -> Socket {
        let result = Socket::new(Domain::IPV4, Type::DGRAM, Option::Some(Protocol::UDP));
        match result {
            Ok(socket) => {
                socket.set_reuse_address(true);
                socket.set_broadcast(true);
                socket.set_reuse_port(true);

                let broadcast_address = ip_network.broadcast();

                if broadcast_address.is_multicast() {
                    socket.set_multicast_ttl_v4(2);
                    socket.set_multicast_loop_v4(true);

                    match broadcast_address {
                        IpAddr::V4(addr) => {
                            socket.join_multicast_v4(&addr, &Ipv4Addr::UNSPECIFIED);
                        }
                        IpAddr::V6(_) => {}
                    }
                }

                socket.bind(&SockAddr::from(SocketAddrV4::new(
                    Ipv4Addr::UNSPECIFIED,
                    socket_port,
                )));

                return socket;
            }
            Err(e) => {
                std::panic::panic_any(e);
            }
        }
    }

    fn find_available_network() -> IpNetwork {
        let network_interfaces = pnet::datalink::interfaces();

        for interface in network_interfaces {
            if interface.is_loopback() || interface.is_point_to_point() {
                continue;
            }
            for ip_network in interface.ips {
                if ip_network.is_ipv4() {
                    return ip_network;
                }
            }
        }

        return IpNetwork::from(Ipv4Network::from(Ipv4Addr::LOCALHOST));
    }
}

#[cfg(unix)]
impl ZHandler for ZBeacon {
    fn handle(&mut self, _: Arc<Context>, pipe: &zmq::Socket) {
        // let pipe = Rc::new(pipe);
        let mut poll_items = [
            pipe.as_poll_item(POLLIN),
            PollItem::from_fd(self.socket.as_raw_fd(), POLLIN),
        ];
        info!("ZBeacon Started: {}", self.network.to_string());

        while !self.is_terminated {
            let mut timeout = 1;

            if self.transmit.is_some() {
                timeout = self.ping_at.sub(Instant::now()).as_millis();
            }

            let _result = zmq::poll(&mut poll_items, timeout as i64)
                .expect("Error while polling in ZBeacon!");

            // Event from pipe
            if poll_items[0].get_revents() == POLLIN {
                self.handle_pipe(pipe);
            }

            // Event from udp socket
            if poll_items[1].get_revents() == POLLIN {
                self.handle_udp_socket(pipe)
            }

            if self.transmit.is_some() && self.ping_at <= Instant::now() {
                self.send_beacon();
                self.ping_at = Instant::now() + self.interval;
            }
        }
    }
}

impl ZBeacon {
    fn handle_pipe(&mut self, pipe: &zmq::Socket) {
        let result = pipe.recv_multipart(0);

        match result {
            Ok(mut request) => {
                if request.len() < 1 {
                    warn!("Invalid request!");
                    return;
                }
                let vec = request.remove(0);

                let command = convert_bytes_to_string(vec.clone());
                debug!("ZBeacon command: {}", command);

                match command.as_str() {
                    PUBLISH_COMMAND => {
                        if request.len() < 1 {
                            warn!("Invalid request!");
                            return;
                        }

                        self.transmit = Option::Some(request.remove(0));

                        // send beacon instantly
                        self.ping_at = Instant::now();
                    }
                    SILENCE_COMMAND => {
                        self.transmit = Option::None;
                    }
                    SUBSCRIBE_COMMAND => {
                        if request.len() < 1 {
                            warn!("Invalid request!");
                            return;
                        }

                        self.filter = Option::Some(request.remove(0));
                        self.apply_filter = true;
                    }
                    UNSUBSCRIBE_COMMAND => {
                        self.filter = Option::None;
                        self.apply_filter = true;
                    }
                    TERMINATE_COMMAND => {
                        self.is_terminated = true;
                    }
                    _ => warn!("ZBeacon - Invalid command: {}", command),
                }
            }
            Err(e) => {
                panic!(e)
            }
        }
    }

    fn handle_udp_socket(&self, pipe: &zmq::Socket) {
        // nothing to do yet
        info!("Receive some message on udp socket");
        let mut uninit_buffer = [MaybeUninit::<u8>::uninit(); 255];
        let result = self.socket.recv_from(&mut uninit_buffer);

        match result {
            Ok((buffer_size, sock_addr)) => {
                let ipv4_sock_addr = sock_addr.as_socket_ipv4().unwrap().to_string();

                let mut is_valid_transmit = true;
                let buffer = convert_uninit_buffer_to_vec(uninit_buffer, buffer_size);

                // apply filters
                if self.apply_filter {
                    if self.filter.is_none() {
                        return;
                    }

                    is_valid_transmit = buffer.starts_with(self.filter.as_ref().unwrap().as_slice());
                }

                if is_valid_transmit && self.transmit.is_some() {
                    if check_if_vec_equal(&buffer, &self.transmit.as_ref().unwrap()) {
                        return;
                    }
                }

                info!("{}: Receive data from {} with size {}", thread_id::get(), ipv4_sock_addr, buffer_size);

                if is_valid_transmit {
                    pipe.send(ipv4_sock_addr.as_str(), zmq::SNDMORE);
                    pipe.send(buffer, zmq::DONTWAIT);
                }
            }
            Err(e) => {
                error!("Error reading data from udp socket: {}", e)
            }
        }
    }

    fn send_beacon(&mut self) {
        if self.transmit.is_none() {
            return;
        }

        debug!("Sending ZBeacon to {}", self.network.broadcast().to_string());
        let broadcast_socket_addr = SockAddr::from(SocketAddr::new(self.network.broadcast(), self.port));

        let result = self.socket.send_to(
            self.transmit.clone().unwrap().as_slice(),
            &broadcast_socket_addr,
        );

        match result {
            Ok(_) => {
                // ignore
                debug!("Sent message successfully!")
            }
            Err(e) => {
                error!("ZBeacon failed to send beacon: {}", e);

                let os_error_code = e.raw_os_error();
                if os_error_code.is_none()
                    || (os_error_code.unwrap() != ENET_DOWN && os_error_code.unwrap() != ENET_UNREACHED) {
                    self.is_terminated = true;
                }
            }
        }
    }
}
