const ZRE_DISCOVERY_PORT: i32 = 5670;

pub struct ZreNode {
    ctx: zmq::Context,
    beacon_port: i32,
}

impl ZreNode {
    #[inline]
    pub fn new() {}

    pub fn init(&self) {}
}
