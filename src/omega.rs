use std::net::SocketAddr;
use std::io::Error;
use std::io;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU32, Ordering, AtomicBool};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::rc::Rc;

use rand::Rng;
use bincode::ErrorKind;

use crate::config::Config;
use crate::transport::{NetTransport, Transport};
use crate::state::Alive;
use crate::awareness::Awareness;
use crate::queue::TransmitLimitedQueue;
use crate::broadcast::OmegaBroadcast;
use crate::models::{NodeState, Node, NodeStateKind, Alive, encode, MessageKind};
use crate::models::NodeStateKind::{Left, Dead};
use crate::models::MessageKind::AliveMessage;

pub struct Omega {
    sequence_num: AtomicU32,
    incarnation: AtomicU32,

    advertise_addr: SocketAddr,

    config: Config,
    leave: AtomicBool,

    transport: dyn Transport,

    awareness: Awareness,

    broadcast: TransmitLimitedQueue,

    nodes: Vec<NodeState>,
    nodes_map: HashMap<String, NodeState>
}

impl Omega {
    pub fn new(config: Config) {

    }

    fn create_omega(config: Config) -> Omega {
        
    }

    fn set_alive(&mut self) -> io::Result<()> {
        let result = self.refresh_advertise();
        return match result {
            Ok(addr) => {
                self.alive_node(Alive {
                    incarnation: self.next_incarnation(),
                    node: self.config.node.clone(),
                    addr
                });
                Ok(())
            }
            Err(e) => {
                e
            }
        }
    }

    fn refresh_advertise(&mut self) -> io::Result<SocketAddr> {
        let result = self.transport.find_advertise_addr();
        match result {
            Ok(addr) => {
                self.advertise_addr = addr;
                Ok(self.advertise_addr.clone())
            }
            Err(e) => {
                e
            }
        }
    }

    fn is_left(&self) -> bool {
        return self.leave.load(Ordering::Acquire);
    }
}

impl Omega {
    fn encode_and_broadcast<T: ?Sized>(&mut self, node: String, kind: MessageKind, msg: &T) {
        self.encode_broadcast_and_notify(node, kind, msg, None);
    }

    fn encode_broadcast_and_notify<T: ?Sized>(&mut self, node: String, kind: MessageKind, msg: &T, notify: Option<Sender<bool>>) {
        let result = encode(kind, msg);
        match result {
            Ok(bytes) => {
                self.queue_broadcast(node, bytes, notify);
            }
            Err(_) => {}
        }
    }

    fn queue_broadcast(&mut self, node: String, msg: Vec<u8>, notify: Option<Sender<bool>>) {
        let broadcast = OmegaBroadcast{
            node,
            msg,
            notify
        };
        self.broadcast.queue_broadcast(Rc::new(broadcast));
    }
}

impl Omega {
    fn next_sequence_no(&mut self) -> u32
    {
        return self.sequence_num.fetch_add(1, Ordering::Acquire);
    }

    fn next_incarnation(&mut self) -> u32
    {
        return self.incarnation.fetch_add(1, Ordering::Acquire);
    }

    fn skip_incarnation(&mut self, offset: u32) -> u32
    {
        return self.incarnation.fetch_add(offset, Ordering::Acquire);
    }

    fn alive_node(&mut self, alive: Alive, bootstrap: bool, notify: Option<Sender<bool>>) {
        let mut update_nodes = false;
        let is_local_node = alive.node == self.config.node.clone();
        let mut state: NodeState;

        if self.is_left() && is_local_node {
            return;
        }

        if let Some(node_state) = self.nodes_map.get_mut(&alive.node) {
            if alive.addr != node_state.node.addr {
                let can_reclaim = self.config.dead_node_reclaim_time > Duration::ZERO
                    && self.config.dead_node_reclaim_time <= Instant::now().duration_since(node_state.state_change);

                if node_state.state == Left
                    || (node_state.state == Dead && can_reclaim) {
                    update_nodes = true;
                } else {
                    return;
                }
            }
            &state = node_state;
        } else {
            state = NodeState {
                node: Node {
                    name: alive.node.clone(),
                    addr: alive.addr,
                    state: Dead
                },
                state: Dead,
                incarnation: alive.incarnation,
                state_change: Instant::now()
            };

            let n = self.nodes.len();
            let offset = rand::thread_rng().gen_range(0..n);

            self.nodes_map.insert(alive.node.clone(), state);
            self.nodes.push(node_state.clone());
            self.nodes.swap(offset, n);
        }

        if alive.incarnation <= state.incarnation && !is_local_node && !update_nodes {
            return;
        }

        if alive.incarnation <= state.incarnation && is_local_node {
            return;
        }

        // Todo - Clear out any suspicion timer

        if is_local_node && !bootstrap {
            self.refute(&mut state, alive.incarnation)
        } else {
            // broadcast and notify
            self.encode_broadcast_and_notify(state.get_name(), AliveMessage, &alive, notify);

            state.incarnation = alive.incarnation;
            state.node.addr = alive.addr;
            if state.state != NodeStateKind::Alive {
                state.state = NodeStateKind::Alive;
                state.state_change = Instant::now();
            }
        }
    }

    fn refute(&mut self, node_state: &mut NodeState, accused_inc: u32) {
        let mut inc = self.next_incarnation();
        if accused_inc >= inc {
            inc = self.skip_incarnation(accused_inc - inc + 1);
        }

        node_state.incarnation = inc;

        self.awareness.apply_delta(1);
        
        let alive = Alive {
            incarnation: inc,
            node: node_state.get_name(),
            addr: node_state.get_addr()
        };

        // encode and broadcast
        self.encode_and_broadcast(node_state.get_name(), AliveMessage, &alive);
    }
}