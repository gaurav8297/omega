use omega::zactor::ZActor;
use omega::zbeacon::ZBeacon;
use omega::zmessage::create_beacon_message;
use std::sync::{Arc, Mutex, Once};

static mut NODE_A: Option<ZActor> = Option::None;
static mut NODE_B: Option<ZActor> = Option::None;

static INIT: Once = Once::new();
static PORT: u16 = 9999;

fn init()
{
    env_logger::init();

    unsafe {
        INIT.call_once(|| {
            let (node_a, node_b) = setup_nodes();
            NODE_A = Option::Some(node_a);
            NODE_B = Option::Some(node_b)
        });
    }
}

fn setup_nodes() -> (ZActor, ZActor)
{
    let context = Arc::new(zmq::Context::new());
    let handler_a: ZBeacon = ZBeacon::new(PORT);
    let mut node_a = ZActor::new(context.clone(), Arc::new(Mutex::new(handler_a)));
    node_a.start();

    let handler_b: ZBeacon = ZBeacon::new(PORT);
    let mut node_b = ZActor::new(context.clone(), Arc::new(Mutex::new(handler_b)));
    node_b.start();

    return (node_a, node_b)
}

unsafe fn node_a() -> ZActor
{
    NODE_A.clone().unwrap()
}

unsafe fn node_b() -> ZActor
{
    NODE_B.clone().unwrap()
}

#[test]
fn test_zbeacon()
{
    init();

    let message_a = create_beacon_message(PORT);
    let message_b = create_beacon_message(PORT);

    unsafe {
        let node_a = node_a();
        let node_b = node_b();

        node_a.send("PUBLISH", zmq::SNDMORE);
        node_a.send(message_a, zmq::DONTWAIT);
        node_b.send("PUBLISH", zmq::SNDMORE);
        node_b.send(message_b, zmq::DONTWAIT);
    }
}
