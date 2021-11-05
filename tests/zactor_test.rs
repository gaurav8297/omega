use log::info;
use omega::zactor::{ZHandler};
use std::sync::Arc;
use std::thread::sleep;
use std::time;
use zmq::{Context, Socket};

pub struct TestHandler;

impl ZHandler for TestHandler {
    fn handle(&mut self, _: Arc<Context>, _: &Socket) {
        sleep(time::Duration::from_millis(1));
        info!("aaaaa")
    }
}

// #[test]
// fn test_zactor() {
//     let handler: TestHandler = TestHandler {};
//     let context: Context = zmq::Context::new();
//     let mut actor = ZActor::new(Arc::new(context), Arc::new(handler));
//     let handle = actor.start();
//     handle.join();
// }

#[test]
fn test_something() {
    let nets = pnet::datalink::interfaces();
    for n in nets.iter() {
        for x in n.ips.iter() {
            println!("========IPS=======");
            println!("{}", x.network());
            println!("{}", x.broadcast());
            println!("{}", x.is_ipv4());
            println!("{}", n.is_loopback());
            println!("=======IPS========");
        }
        println!("{}", n.is_loopback());
        println!("{}", n.is_point_to_point());
        println!("=======END========")
    }
}
