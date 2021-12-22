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
use crate::broadcast::{Broadcast, DummyBroadcast};
use std::collections::btree_set::Iter;
use std::iter::Cloned;

pub struct TransmitLimitedQueue
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
        if self.eq(other) {
            return Ordering::Equal;
        }

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

    pub fn queue_broadcast(&mut self, broadcast: Rc<impl Broadcast>) {
        self._queue_broadcast(broadcast, 0);
    }

    pub fn _queue_broadcast(&mut self, broadcast: Rc<impl Broadcast>, initial_transmits: usize)
    {
        let queue = self.transmit_map.get_mut().unwrap();
        let mut remove = vec![];

        if self.id_gen == u64::MAX {
            self.id_gen = 0;
        } else {
            self.id_gen += 1;
        }

        queue.iter()
            .for_each(|shared_lb| {
                let lb = shared_lb.borrow();
                if lb.broadcast.invalidates(Box::new(broadcast.clone())) {
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
        self.cleanup_id_gen();
    }

    pub fn find_broadcasts(&mut self, overhead: usize, limit: usize) -> Option<Vec<Vec<u8>>>
    {
        let queue = self.transmit_map.get_mut().unwrap();
        if queue.is_empty() {
            return Option::None;
        }

        let transmit_limit = retransmit_limit(self.retransmit_mul, (self.num_nodes)());

        let min_transmit = get_min(&queue).borrow().transmit;
        let max_transmit = get_max(&queue).borrow().transmit;

        let mut byte_used: usize = 0;
        let mut re_insert: Vec<Rc<RefCell<LimitedBroadcast>>> = vec![];
        let mut ret: Vec<Vec<u8>> = vec![];
        let mut transmit = min_transmit;

        while transmit <= max_transmit {
            let free = limit as i64 - byte_used as i64 - overhead as i64;
            if free <= 0 {
                break;
            }

            let greater_or_equal = LimitedBroadcast {
                transmit,
                msg_len: free as usize,
                id: u64::MAX,
                broadcast: Rc::new(DummyBroadcast::new()),
                name: None
            };

            let less_than = LimitedBroadcast {
                transmit: transmit + 1,
                msg_len: usize::MAX,
                id: u64::MAX,
                broadcast: Rc::new(DummyBroadcast::new()),
                name: None
            };

            let item = queue.range((
                    Included(Rc::new(RefCell::new(greater_or_equal))),
                    Excluded(Rc::new(RefCell::new(less_than)))))
                .find(|lb| lb.borrow().msg_len <= free as usize);

            if item.is_none() {
                transmit += 1;
                continue;
            }

            let shared_lb = item.unwrap().clone();
            queue.remove(&shared_lb);

            let mut lb = shared_lb.borrow_mut();
            byte_used += overhead + lb.msg_len;
            ret.push(lb.broadcast.message());

            if lb.transmit + 1 >= transmit_limit {
                lb.broadcast.finished();
            } else {
                lb.transmit += 1;
                re_insert.push(shared_lb.clone());
            }
        }

        re_insert.iter()
            .for_each(|shared_lb| {
                queue.insert(shared_lb.clone());
            });

        self.cleanup_id_gen();
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

        self.cleanup_id_gen();
    }

    fn cleanup_id_gen(&mut self) {
        let queue = self.transmit_map.lock().unwrap();
        if queue.len() == 0 {
            self.id_gen = 0;
        }
    }

    pub fn len(&self) -> usize {
        return self.transmit_map.lock().unwrap().len();
    }

    pub fn collect(&self) -> Vec<Rc<RefCell<LimitedBroadcast>>> {
        let queue = self.transmit_map.lock().unwrap();
        return queue.iter().cloned().collect();
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
