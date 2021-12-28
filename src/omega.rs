use std::net::SocketAddr;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering, AtomicBool};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::rc::Rc;
use std::cell::RefCell;

use rand::Rng;
use tokio::sync::mpsc::UnboundedSender;
use serde::Serialize;

use crate::config::Config;
use crate::transport::{Transport};
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

    transport: Transport,

    awareness: Awareness,

    broadcast: TransmitLimitedQueue,

    nodes: Vec<Rc<RefCell<NodeState>>>,
    nodes_map: HashMap<String, Rc<RefCell<NodeState>>>
}

impl Omega {
    // pub fn new(config: Config) -> Omega
    // {
    //
    // }
    //
    // fn create_omega(config: Config) -> Omega
    // {
    //
    // }

    fn set_alive(&mut self, notify: Option<UnboundedSender<()>>) -> io::Result<()>
    {
        let addr = self.refresh_advertise()?;
        let alive = Alive {
            incarnation: self.next_incarnation(),
            node: self.config.node.clone(),
            addr
        };
        self.alive_node(alive, false, notify);
        return Ok(());
    }

    fn refresh_advertise(&mut self) -> io::Result<SocketAddr>
    {
        self.advertise_addr = self.transport.find_advertise_addr()?;
        return Ok(self.advertise_addr.clone());
    }

    fn is_left(&self) -> bool
    {
        return self.leave.load(Ordering::Acquire);
    }
}

impl Omega
{
    fn encode_and_broadcast<T: ?Sized>(&mut self, node: String, kind: MessageKind, msg: &T)
    where
        T: Serialize
    {
        self.encode_broadcast_and_notify(node, kind, msg, None);
    }

    fn encode_broadcast_and_notify<T: ?Sized>(&mut self,
                                              node: String,
                                              kind: MessageKind,
                                              msg: &T,
                                              notify: Option<UnboundedSender<()>>)
    where
        T: Serialize
    {
        let result = encode(kind, msg);
        match result {
            Ok(bytes) => {
                self.queue_broadcast(node, bytes, notify);
            }
            Err(_) => {}
        }
    }

    fn queue_broadcast(&mut self, node: String, msg: Vec<u8>, notify: Option<UnboundedSender<()>>)
    {
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

    fn alive_node(&mut self, alive: Alive, bootstrap: bool, notify: Option<UnboundedSender<()>>)
    {
        let mut update_nodes = false;
        let is_local_node = alive.node == self.config.node.clone();
        let mut shared_state;

        if self.is_left() && is_local_node {
            return;
        }

        if let Some(node_state) = self.nodes_map.get(&alive.node) {
            let state = node_state.borrow();
            if alive.addr != state.node.addr {
                let can_reclaim = self.config.dead_node_reclaim_time > Duration::ZERO
                    && self.config.dead_node_reclaim_time <= Instant::now().duration_since(state.state_change);

                if state.state == Left
                    || (state.state == Dead && can_reclaim) {
                    update_nodes = true;
                } else {
                    return;
                }
            }
            shared_state = node_state.clone();
        } else {
            let node_state = NodeState {
                node: Node {
                    name: alive.node.clone(),
                    addr: alive.addr,
                    state: Dead
                },
                state: Dead,
                incarnation: alive.incarnation,
                state_change: Instant::now()
            };
            shared_state = Rc::new(RefCell::new(node_state));

            let n = self.nodes.len();
            let offset = rand::thread_rng().gen_range(0..n);

            self.nodes_map.insert(alive.node.clone(), shared_state.clone());
            self.nodes.push(shared_state.clone());
            self.nodes.swap(offset, n);
        }
        let mut state = shared_state.borrow_mut();

        if alive.incarnation <= state.incarnation && !is_local_node && !update_nodes {
            return;
        }

        if alive.incarnation <= state.incarnation && is_local_node {
            return;
        }

        // Todo - Clear out any suspicion timer

        if is_local_node && !bootstrap {
            self.refute(shared_state.clone(), alive.incarnation)
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

    fn refute(&mut self, shared_state: Rc<RefCell<NodeState>>, accused_inc: u32)
    {
        let mut inc = self.next_incarnation();
        if accused_inc >= inc {
            inc = self.skip_incarnation(accused_inc - inc + 1);
        }

        let mut state = shared_state.borrow_mut();
        state.incarnation = inc;

        self.awareness.apply_delta(1);

        let alive = Alive {
            incarnation: inc,
            node: state.get_name(),
            addr: state.get_addr()
        };

        // encode and broadcast
        self.encode_and_broadcast(state.get_name(), AliveMessage, &alive);
    }
}