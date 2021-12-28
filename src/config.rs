use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub node: String,
    pub advertise_host: String,
    pub advertise_port: u16,
    pub dead_node_reclaim_time: Duration
}