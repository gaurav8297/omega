use uuid::Uuid;
use zmq::{Context, Socket};
use std::rc::Rc;
use std::net::SocketAddrV4;

const PEER_EXPIRED: i32 = 3000;
const PEER_EVASIVE: i32 = 10;

pub struct ZrePeer {
    socket: Socket,
    identity: Uuid,
    sent_sequence: i32,
    want_sequence: i32
}

impl ZrePeer {
    #[inline]
    pub fn new(ctx: Rc<Context>, peer_identity: Uuid, reply_identity: Uuid, endpoint: String) -> ZrePeer
    {
        let socket = ctx.socket(zmq::DEALER).unwrap();
        socket.set_linger(0);
        socket.set_identity(reply_identity.as_bytes());
        socket.set_sndhwm(PEER_EXPIRED);
        socket.set_sndtimeo(0);

        let error_message = format!("Unable to connect to peer: {}", endpoint);
        socket.connect(endpoint.as_str())
            .expect(error_message.as_str());

        return ZrePeer {
            socket,
            identity: peer_identity,
            sent_sequence: 0,
            want_sequence: 0
        }
    }

    pub fn send(&mut self, message: String)
    {
        self.sent_sequence += 1;
        self.sent_sequence = self.sent_sequence % 65535

    }
}