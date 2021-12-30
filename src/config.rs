use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub node: String,
    pub dead_node_reclaim_time: Duration,
    pub retransmit_multiplier: usize,
    pub awareness_max_multiplier: u32,
    pub advertise_host: String,
    pub advertise_port: u16,
}

impl Config {
    pub fn get_socket_addr(&self) -> SocketAddr {
        let mut port = self.advertise_port.clone();
        if port == 0 {
            match portpicker::pick_unused_port() {
                None => {
                    panic!("Unable to find port!");
                }
                Some(p) => {
                    port = p;
                }
            }
        }
        let addr = format!("{}:{}", self.advertise_host, self.advertise_port);
        return SocketAddr::from_str(addr.as_str()).unwrap();
    }
}