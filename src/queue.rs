use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;
use std::cmp::Ordering;
use std::fs::read;
use std::ops::{DerefMut, Deref};
use std::rc::Rc;
use std::ops::Bound::{Included, Excluded};
use crate::util::retransmit_limit;
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};

pub trait Broadcast
{
    fn invalidates(&self, b: Rc<dyn Broadcast>) -> bool;
    fn message(&self) -> Vec<u8>;
    fn finished(&self);
}

pub struct DummyBroadcast;

impl Broadcast for DummyBroadcast {
    fn invalidates(&self, b: Rc<dyn Broadcast>) -> bool {
        return false;
    }

    fn message(&self) -> Vec<u8> {
        return vec![]
    }

    fn finished(&self) {
        return;
    }
}

struct TransmitLimitedQueue
{
    num_nodes: fn() -> usize,
    retransmit_mul: usize,
    transmit_map: Mutex<BTreeSet<Rc<RefCell<LimitedBroadcast>>>>,
    id_gen: u64
}

pub struct LimitedBroadcast
{
    pub transmit: usize,
    pub msg_len: usize,
    pub id: u64,
    pub broadcast: Rc<dyn Broadcast>,
    pub name: Option<String>
}

impl Debug for LimitedBroadcast {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result
    {
        f.debug_tuple("")
            .field(&self.id)
            .field(&self.transmit)
            .field(&self.msg_len)
            .finish()
    }
}

impl PartialEq for LimitedBroadcast {
    fn eq(&self, other: &Self) -> bool {
        if self.transmit == other.transmit
            && self.msg_len == other.msg_len
            && self.id == other.id
            && self.name == other.name {
            return true;
        }
        return false;
    }
}

impl PartialOrd for LimitedBroadcast {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LimitedBroadcast {}

impl Ord for LimitedBroadcast
{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.transmit < other.transmit {
            return Ordering::Less;
        } else if self.transmit > other.transmit {
            return Ordering::Greater;
        }

        if self.msg_len > other.msg_len {
            return Ordering::Less;
        } else if self.msg_len < other.msg_len {
            return Ordering::Greater;
        }

        return if self.id > other.id {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

impl TransmitLimitedQueue
{
    pub fn new(num_nodes: fn() -> usize, retransmit_mul: usize) -> TransmitLimitedQueue
    {
        return TransmitLimitedQueue {
            num_nodes,
            retransmit_mul,
            transmit_map: Mutex::new(BTreeSet::new()),
            id_gen: 0
        }
    }

    pub fn queue_broadcast(&mut self, broadcast: Rc<dyn Broadcast>, initial_transmits: usize)
    {
        let mut queue = self.transmit_map.lock().unwrap();
        let mut remove = vec![];

        if self.id_gen == u64::MAX {
            self.id_gen = 0;
        } else {
            self.id_gen += 1;
        }

        queue.iter().cloned()
            .for_each(|shared_lb| {
                let lb = shared_lb.borrow();
                if lb.broadcast.invalidates(broadcast.clone()) {
                    lb.broadcast.finished();
                    remove.push(shared_lb.clone());
                }
            });

        remove.iter()
            .for_each(|lb| {
                queue.remove(lb);
            });

        let limited_broadcast = LimitedBroadcast {
            transmit: initial_transmits,
            msg_len: broadcast.message().len(),
            id: self.id_gen,
            broadcast: broadcast.clone(),
            name: Option::None
        };

        queue.insert(Rc::new(RefCell::new(limited_broadcast)));
    }

    pub fn get_broadcasts(&self, overhead: usize, limit: usize) -> Option<Vec<Vec<u8>>>
    {
        let mut queue = self.transmit_map.lock().unwrap();
        if queue.is_empty() {
            return Option::None;
        }

        let transmit_limit = retransmit_limit(self.retransmit_mul, (self.num_nodes)());

        let max_transmit = get_max(&queue).borrow().transmit;
        let min_transmit = get_min(&queue).borrow().transmit;

        let mut byte_used: usize = 0;
        let mut ret: Vec<Vec<u8>> = vec![];

        for transmit in min_transmit..max_transmit + 1 {
            let free = (limit - byte_used - overhead) as i64;
            if free <= 0 {
                break;
            }

            let greater_or_equal = LimitedBroadcast {
                transmit,
                msg_len: free as usize,
                id: u64::MAX,
                broadcast: Rc::new(DummyBroadcast{}),
                name: None
            };

            let less_than = LimitedBroadcast {
                transmit: transmit + 1,
                msg_len: usize::MAX,
                id: u64::MAX,
                broadcast: Rc::new(DummyBroadcast{}),
                name: None
            };

            let option = queue.range((
                    Included(Rc::new(RefCell::new(greater_or_equal))),
                    Excluded(Rc::new(RefCell::new(less_than)))))
                .find(|lb| lb.borrow().msg_len <= free as usize);

            if option.is_none() {
                continue;
            }

            let shared_lb = option.unwrap().clone();

            let mut lb = shared_lb.borrow_mut();
            byte_used += overhead + lb.msg_len;
            ret.push(lb.broadcast.message());
            queue.remove(&shared_lb);

            if lb.transmit + 1 >= transmit_limit {
                lb.broadcast.finished();
            } else {
                lb.transmit += 1;
                queue.insert(shared_lb.clone());
            }
        }
        return Some(ret);
    }

    pub fn reset(&mut self)
    {
        let queue = self.transmit_map.lock().unwrap();
        queue.iter()
            .for_each(|shared_lb| {
                let lb = shared_lb.borrow();
                lb.broadcast.finished();
            });

        drop(queue);
        self.transmit_map = Mutex::new(BTreeSet::new());
        self.id_gen = 0;
    }

    pub fn prune(&mut self, max_retain: usize)
    {
        let queue = self.transmit_map.get_mut().unwrap();

        while queue.len() > max_retain {
            let lb = get_max(queue);
            lb.borrow().broadcast.finished();
            queue.remove(&lb);
        }
    }

    pub fn len(&self) -> usize {
        return self.transmit_map.lock().unwrap().len();
    }
}

pub fn get_max(queue: &BTreeSet<Rc<RefCell<LimitedBroadcast>>>) -> Rc<RefCell<LimitedBroadcast>>
{
    return queue.iter().max().unwrap().clone();
}

pub fn get_min(queue: &BTreeSet<Rc<RefCell<LimitedBroadcast>>>) -> Rc<RefCell<LimitedBroadcast>>
{
    return queue.iter().min().unwrap().clone();
}
