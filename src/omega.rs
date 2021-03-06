use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::ops::Mul;
use std::sync::{Arc, RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, SystemTime};

use rand::Rng;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio::sync::broadcast::Sender;

use crate::awareness::Awareness;
use crate::broadcast::OmegaBroadcast;
use crate::config::Config;
use crate::models::{
    AliveMessage,
    DeadMessage,
    decode_from_tcp_stream,
    encode,
    encode_to_tcp_stream,
    MessageKind,
    Node,
    NodeState,
    NodeStateKind,
    PushPullMessage,
    read_message_header};
use crate::models::MessageKind::{AliveMsg, DeadMsg, PingMsg, PushPullMsg, SuspectMsg};
use crate::models::NodeStateKind::{Alive, Dead, Left, Suspect};
use crate::queue::TransmitLimitedQueue;
use crate::suspicion::Suspicion;
use crate::transport::Transport;
use crate::util::suspicion_timeout;

struct Nodes
{
    nodes: Vec<RefCell<NodeState>>,
    node_map: HashMap<String, RefCell<NodeState>>,
}

impl Nodes {
    #[inline]
    fn insert_node(&mut self, node: NodeState)
    {
        if let Some(state) = self.get(&node.get_name()) {
            panic!("Node already present with name {}", state.node.name);
        }

        let nodes_ref = RefCell::new(node.clone());
        let n = self.nodes.len();

        self.node_map.insert(node.get_name(), nodes_ref.clone());
        self.nodes.push(nodes_ref.clone());

        if n > 0 {
            let offset = rand::thread_rng().gen_range(0..n);
            self.nodes.swap(offset, n);
        }
    }

    #[inline]
    fn get_mut(&mut self, name: &String) -> Option<&mut NodeState>
    {
        return self.node_map.get_mut(name)
            .map(|node_ref| node_ref.get_mut());
    }

    #[inline]
    fn get(&self, name: &String) -> Option<NodeState>
    {
        return self.node_map.get(name)
            .map(|node_ref| node_ref.borrow().clone());
    }

    #[inline]
    fn contains(&self, name: &String) -> bool
    {
        return self.node_map.get(name).is_some();
    }

    #[inline]
    fn get_vec(&self) -> Vec<NodeState>
    {
        return self.nodes.iter()
            .map(|node_ref| node_ref.borrow().clone())
            .collect();
    }

    #[inline]
    fn len(&self) -> usize
    {
        return self.nodes.len();
    }
}

pub struct Omega
{
    config: Config,

    sequence_num: AtomicU32,
    incarnation: AtomicU32,
    advertise_addr: SocketAddr,

    leave: AtomicBool,
    leave_chn: Sender<()>,
    shutdown: AtomicBool,
    shutdown_chn: Sender<()>,

    transport: Transport,
    awareness: Awareness,
    broadcast_queue: TransmitLimitedQueue,

    nodes: RwLock<Nodes>,
    node_suspicion: RwLock<HashMap<String, Suspicion>>,
    num_nodes: AtomicUsize,
}

impl Omega {
    pub async fn new(config: Config) -> io::Result<Omega>
    {
        let mut omega: Omega = Self::create_omega(config).await?;
        omega.set_alive()?;

        let omega_ptr = omega.create_atomic_ptr();
        tokio::spawn(async move {
            let o = Self::load_atomic_ptr(omega_ptr);
            o.stream_listen().await;
        });

        return Ok(omega);
    }

    pub fn create_atomic_ptr(&mut self) -> Arc<AtomicPtr<Omega>>
    {
        return Arc::new(AtomicPtr::new(self));
    }

    pub fn load_atomic_ptr<'a>(ptr: Arc<AtomicPtr<Omega>>) -> &'a mut Omega
    {
        return unsafe { ptr.load(Relaxed).as_mut().unwrap() };
    }

    async fn create_omega(config: Config) -> io::Result<Omega>
    {
        let transport = Transport::new(config.get_socket_addr()).await?;
        let advertise_addr = transport.find_advertise_addr()?;

        let awareness = Awareness::new(config.awareness_max_multiplier);
        let broadcast_queue = TransmitLimitedQueue::new(config.retransmit_multiplier);

        let (leave_chn, _) = tokio::sync::broadcast::channel(10);
        let (shutdown_chn, _) = tokio::sync::broadcast::channel(10);

        let omega = Omega {
            config,
            sequence_num: AtomicU32::new(0),
            incarnation: AtomicU32::new(0),
            advertise_addr,
            leave: AtomicBool::new(false),
            leave_chn,
            shutdown: AtomicBool::new(false),
            shutdown_chn,
            transport,
            awareness,
            broadcast_queue,
            nodes: RwLock::new(Nodes {
                nodes: vec![],
                node_map: HashMap::new(),
            }),
            node_suspicion: RwLock::new(HashMap::new()),
            num_nodes: AtomicUsize::new(0),
        };
        return Ok(omega);
    }

    fn set_alive(&mut self) -> io::Result<()>
    {
        let addr = self.refresh_advertise()?;
        let alive = AliveMessage {
            incarnation: self.next_incarnation(),
            node: self.config.node.clone(),
            addr,
        };
        self.alive_node(&alive, true, None);
        return Ok(());
    }

    fn get_num_nodes(&self) -> usize
    {
        return self.num_nodes.load(Relaxed);
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
                                              notify: Option<Sender<()>>)
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

    fn queue_broadcast(&self, node: String, msg: Vec<u8>, notify: Option<Sender<()>>)
    {
        let broadcast = OmegaBroadcast::new(node, msg, notify);
        self.broadcast_queue.queue_broadcast(Arc::new(broadcast));
    }
}

// net changes
impl Omega {
    pub async fn stream_listen(&mut self)
    {
        let omega_ptr = self.create_atomic_ptr();
        let chn = self.transport.stream_channel();
        let mut shutdown_rec = self.shutdown_chn.subscribe();
        loop {
            tokio::select! {
                stream = chn.recv() => {
                    if let Some(s) = stream {
                        let omega_ptr_clone = omega_ptr.clone();
                        tokio::spawn(async move {
                            let o = Self::load_atomic_ptr(omega_ptr_clone);
                            o.handle_conn(s);
                        });
                    }
                }
                _ = shutdown_rec.recv() => {
                    return;
                }
            }
        }
    }

    async fn handle_conn(&self, mut stream: TcpStream)
    {
        let (kind, size) = read_message_header(&mut stream).await.unwrap();

        match kind {
            PushPullMsg => {
                let msg: PushPullMessage = decode_from_tcp_stream(size, &mut stream).await.unwrap();
                // Send local state to the stream
                self.send_local_state(&mut stream, &msg).await;
                // merge to local state
            }
            PingMsg => {}
            _ => {}
        }
    }

    async fn send_local_state(&self, stream: &mut TcpStream, msg: &PushPullMessage) -> io::Result<()>
    {
        let nodes = self.nodes.read().unwrap();
        let out = PushPullMessage {
            node_states: nodes.get_vec(),
            join: msg.join,
        };
        let _ = encode_to_tcp_stream(PushPullMsg, &out, stream).await?;
        return Ok(());
    }
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

    fn alive_node(&self, alive: &AliveMessage, bootstrap: bool, notify: Option<Sender<()>>)
    {
        let mut nodes = self.nodes.write().unwrap();
        let mut node_suspicion = self.node_suspicion.write().unwrap();

        let mut update_nodes = false;
        let is_local_node = alive.node == self.config.node.clone();

        if self.is_left() && is_local_node {
            return;
        }

        if nodes.contains(&alive.node) {
            let state = NodeState {
                node: Node {
                    name: alive.node.clone(),
                    addr: alive.addr,
                    state: Dead,
                },
                state: Dead,
                incarnation: alive.incarnation,
                state_change: SystemTime::now(),
            };

            nodes.insert_node(state);
            self.num_nodes.fetch_add(1, Relaxed);
        }

        let state = nodes.get_mut(&alive.node).unwrap();

        if alive.addr != state.node.addr {
            let can_reclaim = self.config.dead_node_reclaim_time > Duration::ZERO &&
                self.config.dead_node_reclaim_time <= SystemTime::now().duration_since(state.state_change).unwrap();

            if state.state == Left
                || (state.state == Dead && can_reclaim) {
                update_nodes = true;
            } else {
                return;
            }
        }

        if alive.incarnation <= state.incarnation && !is_local_node && !update_nodes {
            return;
        }

        if alive.incarnation <= state.incarnation && is_local_node {
            return;
        }

        // Clear out any suspicion timer
        node_suspicion.remove(&alive.node);

        if is_local_node && !bootstrap {
            self.refute(state, alive.incarnation);
        } else {
            // broadcast and notify
            self.encode_broadcast_and_notify(state.get_name(), AliveMsg, &alive, notify);

            state.incarnation = alive.incarnation;
            state.node.addr = alive.addr;
            if state.state != NodeStateKind::Alive {
                state.state = NodeStateKind::Alive;
                state.state_change = SystemTime::now();
            }
        }
    }

    fn suspect_node(&mut self, suspect: &DeadMessage)
    {
        let omega_ptr = self.create_atomic_ptr();
        let mut nodes = self.nodes.write().unwrap();
        let mut node_suspicion = self.node_suspicion.write().unwrap();

        let optional_state = nodes.get_mut(&suspect.node);
        if optional_state.is_none() {
            return;
        }

        let mut state = optional_state.unwrap();
        if suspect.incarnation < state.incarnation {
            return;
        }

        if let Some(suspicion) = node_suspicion.get_mut(&state.get_name()) {
            if suspicion.confirm(state.get_name()) {
                self.encode_and_broadcast(state.get_name(), SuspectMsg, suspect);
            }
        }

        if state.state != Alive {
            return;
        }

        if state.get_name() == self.config.node {
            self.refute(state, state.incarnation);
            return;
        } else {
            self.encode_and_broadcast(state.get_name(), SuspectMsg, suspect);
        }

        state.incarnation = suspect.incarnation;
        state.state = Suspect;
        let change_time = SystemTime::now();
        state.state_change = change_time;

        let mut total_confirm = self.config.suspicion_multiplier - 2;
        let num_nodes = self.get_num_nodes() as u32;
        if num_nodes - 2 < total_confirm {
            total_confirm = 0;
        }

        let min = suspicion_timeout(self.config.suspicion_multiplier, num_nodes, self.config.probe_interval);
        let max = min.mul(self.config.suspicion_max_timeout_multiplier);

        let suspect_node = suspect.node.clone();
        let timeout_fn = move || {
            let omega = Self::load_atomic_ptr(omega_ptr);
            let nodes = omega.nodes.read().unwrap();
            let mut dead_msg = None;
            if let Some(node_state) = nodes.get(&suspect_node) {
                if node_state.state == Suspect && node_state.state_change == change_time {
                    dead_msg = Some(DeadMessage {
                        incarnation: node_state.incarnation,
                        node: node_state.get_name(),
                        from: omega.config.node.clone(),
                    })
                }
            }
            drop(nodes);

            if dead_msg.is_some() {
                omega.dead_node(&dead_msg.unwrap());
            }
        };

        node_suspicion.insert(
            state.get_name(),
            Suspicion::new(suspect.from.clone(), total_confirm, min, max, timeout_fn));
    }

    fn dead_node(&self, dead: &DeadMessage)
    {
        let mut nodes = self.nodes.write().unwrap();
        let mut node_suspicion = self.node_suspicion.write().unwrap();

        let res = nodes.get_mut(&dead.node);
        if res.is_none() {
            return;
        }

        let mut state = res.unwrap();
        if dead.incarnation < state.incarnation {
            return;
        }

        node_suspicion.remove(&dead.node);

        if state.dead_or_left() {
            return;
        }

        if state.node.name == self.config.node {
            if !self.is_left() {
                self.refute(state, dead.incarnation);
                return;
            }

            self.encode_broadcast_and_notify(dead.node.clone(), DeadMsg, dead, Some(self.leave_chn.clone()));
        } else {
            self.encode_and_broadcast(dead.node.clone(), DeadMsg, dead);
        }

        state.incarnation = dead.incarnation;

        if dead.node == dead.from {
            state.state = Left;
        } else {
            state.state = Dead;
        }

        state.state_change = SystemTime::now();
    }

    fn refute(&self, state: &mut NodeState, accused_inc: u32)
    {
        let mut inc = self.next_incarnation();
        if accused_inc >= inc {
            inc = self.skip_incarnation(accused_inc - inc + 1);
        }

        state.incarnation = inc;

        self.awareness.apply_delta(1);

        let alive = AliveMessage {
            incarnation: inc,
            node: state.get_name(),
            addr: state.get_addr(),
        };

        // encode and broadcast
        self.encode_and_broadcast(state.get_name(), AliveMsg, &alive);
    }

    fn merge_state(&mut self, remote_states: Vec<NodeState>)
    {
        remote_states.iter().for_each(|state| {
            match state.state {
                Alive => {
                    let alive = AliveMessage {
                        incarnation: state.incarnation,
                        node: state.get_name(),
                        addr: state.get_addr(),
                    };
                    self.alive_node(&alive, false, None);
                }
                Suspect | Dead => {
                    let suspect = DeadMessage {
                        incarnation: state.incarnation,
                        node: state.get_name(),
                        from: state.get_name(),
                    };
                    self.suspect_node(&suspect);
                }
                Left => {
                    let dead = DeadMessage {
                        incarnation: state.incarnation,
                        node: state.get_name(),
                        from: state.get_name(),
                    };
                    self.suspect_node(&dead);
                }
            }
        });
    }
}
