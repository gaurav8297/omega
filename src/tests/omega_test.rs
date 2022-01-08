use crate::config::Config;
use std::time::Duration;
use crate::omega::Omega;
use std::thread;

#[tokio::test]
async fn test_omega_create()
{
    let config = Config {
        node: String::from("ss"),
        dead_node_reclaim_time: Duration::from_millis(10),
        retransmit_multiplier: 0,
        awareness_max_multiplier: 0,
        advertise_host: String::from("127.0.0.1"),
        advertise_port: 0,
    };

    let o = Omega::new(config).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1)).await
}
