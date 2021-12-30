use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::ops::Bound::{Excluded, Included};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, AtomicU64};
use std::sync::atomic::Ordering::Relaxed;

use crate::broadcast::{Broadcast, DummyBroadcast};
use crate::util::retransmit_limit;

pub struct TransmitLimitedQueue
{
    retransmit_mul: usize,
    transmit_map: Mutex<BTreeSet<Arc<LimitedBroadcast>>>,
    id_gen: AtomicU64,
}

pub struct LimitedBroadcast
{
    id: u64,
    transmit: AtomicUsize,
    broadcast: Arc<dyn Broadcast + Sync + Send>,
    msg_len: usize,
}

impl LimitedBroadcast
{
    pub fn new(id: u64,
               transmit: usize,
               msg_len: usize,
               broadcast: Arc<dyn Broadcast + Sync + Send>) -> LimitedBroadcast {
        return LimitedBroadcast {
            id,
            transmit: AtomicUsize::from(transmit),
            broadcast,
            msg_len,
        };
    }

    pub fn get_transmit(&self) -> usize {
        return self.transmit.load(Relaxed);
    }

    pub fn increment_transmit(&self) {
        self.transmit.fetch_add(1, Relaxed);
    }

    pub fn get_message(&self) -> Arc<Vec<u8>>
    {
        return self.broadcast.message();
    }
}

impl Debug for LimitedBroadcast
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result
    {
        f.debug_tuple("")
            .field(&self.id)
            .field(&self.transmit)
            .field(&self.msg_len)
            .finish()
    }
}

impl PartialEq for LimitedBroadcast
{
    fn eq(&self, other: &Self) -> bool
    {
        if self.get_transmit() == other.get_transmit()
            && self.msg_len == other.msg_len
            && self.id == other.id {
            return true;
        }
        return false;
    }
}

impl PartialOrd for LimitedBroadcast
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for LimitedBroadcast
{}

impl Ord for LimitedBroadcast
{
    fn cmp(&self, other: &Self) -> Ordering
    {
        if self.eq(other) {
            return Ordering::Equal;
        }

        if self.get_transmit() < other.get_transmit() {
            return Ordering::Less;
        } else if self.get_transmit() > other.get_transmit() {
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
        };
    }
}

impl TransmitLimitedQueue
{
    pub fn new(retransmit_mul: usize) -> TransmitLimitedQueue
    {
        return TransmitLimitedQueue {
            retransmit_mul,
            transmit_map: Mutex::new(BTreeSet::new()),
            id_gen: AtomicU64::new(0),
        };
    }

    fn get_id_gen(&self) -> u64 {
        return self.id_gen.load(Relaxed);
    }

    fn reset_id_gen(&self) {
        self.id_gen.store(0, Relaxed);
    }

    fn increment_id_gen(&self) -> u64 {
        return self.id_gen.fetch_add(1, Relaxed);
    }

    pub fn queue_broadcast(&self, broadcast: Arc<impl Broadcast + Sync + Send>)
    {
        self._queue_broadcast(broadcast, 0);
    }

    pub fn _queue_broadcast(&self, broadcast: Arc<impl Broadcast + Sync + Send>, initial_transmits: usize)
    {
        let mut queue = self.transmit_map.lock().unwrap();
        let mut remove = vec![];

        if self.get_id_gen() == u64::MAX {
            self.reset_id_gen();
        } else {
            self.increment_id_gen();
        }

        queue.iter()
            .for_each(|lb| {
                if lb.broadcast.invalidates(broadcast.clone()) {
                    lb.broadcast.finished();
                    remove.push(lb.clone());
                }
            });

        remove.iter()
            .for_each(|lb| {
                queue.remove(lb);
            });

        let limited_broadcast = LimitedBroadcast::new(
            self.get_id_gen(),
            initial_transmits,
            broadcast.message().len(),
            broadcast.clone());

        queue.insert(Arc::new(limited_broadcast));
        self.cleanup_id_gen(&queue);
    }

    pub fn find_broadcasts(&self, num_nodes: usize, overhead: usize, limit: usize) -> Option<Vec<Arc<Vec<u8>>>>
    {
        let mut queue = self.transmit_map.lock().unwrap();
        if queue.is_empty() {
            return None;
        }

        let transmit_limit = retransmit_limit(self.retransmit_mul, num_nodes);

        let min_transmit = get_min(&queue).get_transmit();
        let max_transmit = get_max(&queue).get_transmit();

        let mut byte_used = 0;
        let mut re_insert = vec![];
        let mut ret = vec![];
        let mut transmit = min_transmit;

        while transmit <= max_transmit {
            let free = limit as i64 - byte_used as i64 - overhead as i64;
            if free <= 0 {
                break;
            }

            let greater_or_equal = LimitedBroadcast::new(
                u64::MAX, transmit,
                free as usize,
                Arc::new(DummyBroadcast::new()));

            let less_than = LimitedBroadcast::new(
                u64::MAX,
                transmit + 1,
                usize::MAX,
                Arc::new(DummyBroadcast::new()));


            let item = queue.range((
                Included(Arc::new(greater_or_equal)),
                Excluded(Arc::new(less_than))))
                .find(|lb| lb.msg_len <= free as usize);

            if item.is_none() {
                transmit += 1;
                continue;
            }

            let lb = item.unwrap().clone();
            queue.remove(&lb);

            byte_used += overhead + lb.msg_len;
            ret.push(lb.broadcast.message());

            if lb.get_transmit() + 1 >= transmit_limit {
                lb.broadcast.finished();
            } else {
                lb.increment_transmit();
                re_insert.push(lb.clone());
            }
        }

        re_insert.iter()
            .for_each(|lb| {
                queue.insert(lb.clone());
            });

        self.cleanup_id_gen(&queue);
        return Some(ret);
    }

    pub fn reset(&mut self)
    {
        let queue = self.transmit_map.lock().unwrap();
        queue.iter()
            .for_each(|lb| {
                lb.broadcast.finished();
            });

        drop(queue);
        self.transmit_map = Mutex::new(BTreeSet::new());
        self.reset_id_gen();
    }

    pub fn prune(&self, max_retain: usize)
    {
        let mut queue = self.transmit_map.lock().unwrap();

        while queue.len() > max_retain {
            let lb = get_max(&queue);
            lb.broadcast.finished();
            queue.remove(&lb);
        }

        self.cleanup_id_gen(&queue);
    }

    fn cleanup_id_gen(&self, queue: &BTreeSet<Arc<LimitedBroadcast>>)
    {
        if queue.len() == 0 {
            self.reset_id_gen();
        }
    }

    pub fn len(&self) -> usize
    {
        return self.transmit_map.lock().unwrap().len();
    }

    pub fn collect(&self) -> Vec<Arc<LimitedBroadcast>>
    {
        let queue = self.transmit_map.lock().unwrap();
        return queue.iter().cloned().collect();
    }
}

pub fn get_max(queue: &BTreeSet<Arc<LimitedBroadcast>>) -> Arc<LimitedBroadcast>
{
    return queue.iter().max().unwrap().clone();
}

pub fn get_min(queue: &BTreeSet<Arc<LimitedBroadcast>>) -> Arc<LimitedBroadcast>
{
    return queue.iter().min().unwrap().clone();
}
