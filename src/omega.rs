use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use rand::Rng;
use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;

use crate::awareness::Awareness;
use crate::broadcast::OmegaBroadcast;
use crate::config::Config;
use crate::models::{Alive, encode, MessageKind, Node, NodeState, NodeStateKind};
use crate::models::MessageKind::AliveMessage;
use crate::models::NodeStateKind::{Dead, Left};
use crate::queue::TransmitLimitedQueue;
use crate::transport::Transport;

struct Nodes
{
    nodes: Vec<NodeState>,
    nodes_map: HashMap<String, NodeState>,
}

impl Nodes {
    fn upsert_node(&mut self, node: NodeState)
    {
        if let Some(old_state) = self.get_node(&node.get_name()) {
            if let Some(pos) = self.nodes.iter().position(|x| *x == old_state.clone()) {
                self.nodes.remove(pos);
            }
        }

        let n = self.nodes.len();
        self.nodes_map.insert(node.get_name(), node.clone());
        self.nodes.push(node.clone());

        if n > 0 {
            let offset = rand::thread_rng().gen_range(0..n);
            self.nodes.swap(offset, n);
        }
        return;
    }

    fn len(&self) -> usize
    {
        return self.nodes.len();
    }

    fn get_node(&self, node_name: &String) -> Option<&NodeState>
    {
        return self.nodes_map.get(node_name);
    }
}

pub struct Omega
{
    config: Config,
    sequence_num: AtomicU32,
    incarnation: AtomicU32,
    advertise_addr: SocketAddr,
    leave: AtomicBool,
    transport: Transport,
    awareness: Awareness,
    broadcast_queue: TransmitLimitedQueue,
    nodes: RwLock<Nodes>,
    num_nodes: AtomicUsize,
}

impl Omega {
    pub async fn new(config: Config) -> io::Result<Omega>
    {
        let mut omega = Self::create_omega(config).await?;
        omega.set_alive()?;

        let mut omega_ref = Arc::new(AtomicPtr::new(&mut omega));
        tokio::spawn(async move {
            let o = unsafe { omega_ref.load(Relaxed).as_mut().unwrap() };
            o.stream_listen().await;
        });
        return Ok(omega);
    }

    async fn create_omega(config: Config) -> io::Result<Omega>
    {
        let transport = Transport::new(config.get_socket_addr()).await?;
        let advertise_addr = transport.find_advertise_addr()?;

        let awareness = Awareness::new(config.awareness_max_multiplier);
        let broadcast_queue = TransmitLimitedQueue::new(config.retransmit_multiplier);

        let omega = Omega {
            config,
            sequence_num: AtomicU32::new(0),
            incarnation: AtomicU32::new(0),
            advertise_addr,
            leave: AtomicBool::new(false),
            transport,
            awareness,
            broadcast_queue,
            num_nodes: AtomicUsize::new(0),
            nodes: RwLock::new(Nodes {
                nodes: vec![],
                nodes_map: HashMap::new(),
            }),
        };
        return Ok(omega);
    }

    fn set_alive(&mut self) -> io::Result<()>
    {
        let addr = self.refresh_advertise()?;
        let alive = Alive {
            incarnation: self.next_incarnation(),
            node: self.config.node.clone(),
            addr,
        };
        self.alive_node(alive, true, None);
        return Ok(());
    }

    fn refresh_advertise(&self) -> io::Result<SocketAddr>
    {
        return Ok(self.transport.find_advertise_addr()?);
    }

    fn is_left(&self) -> bool
    {
        return self.leave.load(Ordering::Acquire);
    }

    fn update_num_nodes(&self, val: usize)
    {
        self.num_nodes.store(val, Relaxed);
    }
}

impl Omega
{
    fn encode_and_broadcast<T: ?Sized>(&self, node: String, kind: MessageKind, msg: &T)
        where
            T: Serialize
    {
        self.encode_broadcast_and_notify(node, kind, msg, None);
    }

    fn encode_broadcast_and_notify<T: ?Sized>(&self,
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

    fn queue_broadcast(&self, node: String, msg: Vec<u8>, notify: Option<UnboundedSender<()>>)
    {
        let broadcast = OmegaBroadcast::new(node, msg, notify);
        self.broadcast_queue.queue_broadcast(Arc::new(broadcast));
    }
}

// net changes
impl Omega {
    pub async fn stream_listen(&mut self)
    {
        print!("inside stream listen");
        return;
    }

    async fn handle_conn(&self) {}
}

// State changes
impl Omega {
    fn next_sequence_no(&self) -> u32
    {
        return self.sequence_num.fetch_add(1, Ordering::Acquire);
    }

    fn next_incarnation(&self) -> u32
    {
        return self.incarnation.fetch_add(1, Ordering::Acquire);
    }

    fn skip_incarnation(&self, offset: u32) -> u32
    {
        return self.incarnation.fetch_add(offset, Ordering::Acquire);
    }

    fn alive_node(&mut self, alive: Alive, bootstrap: bool, notify: Option<UnboundedSender<()>>)
    {
        let mut nodes = self.nodes.write().unwrap();

        let mut update_nodes = false;
        let is_local_node = alive.node == self.config.node.clone();
        let mut state;

        if self.is_left() && is_local_node {
            return;
        }

        if let Some(node_state) = nodes.get_node(&alive.node) {
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
            state = node_state.clone();
        } else {
            state = NodeState {
                node: Node {
                    name: alive.node.clone(),
                    addr: alive.addr,
                    state: Dead,
                },
                state: Dead,
                incarnation: alive.incarnation,
                state_change: Instant::now(),
            };

            nodes.upsert_node(state.clone());
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
        nodes.upsert_node(state);
    }

    fn refute(&self, state: &mut NodeState, accused_inc: u32)
    {
        let mut inc = self.next_incarnation();
        if accused_inc >= inc {
            inc = self.skip_incarnation(accused_inc - inc + 1);
        }

        state.incarnation = inc;

        self.awareness.apply_delta(1);

        let alive = Alive {
            incarnation: inc,
            node: state.get_name(),
            addr: state.get_addr(),
        };

        // encode and broadcast
        self.encode_and_broadcast(state.get_name(), AliveMessage, &alive);
    }
}
