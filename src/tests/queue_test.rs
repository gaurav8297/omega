use std::collections::BTreeSet;
use std::sync::Arc;

use crate::broadcast::{Broadcast, DummyBroadcast};
use crate::queue::{get_max, get_min, LimitedBroadcast, TransmitLimitedQueue};

#[test]
fn test_limited_broadcast_order()
{
    let min = LimitedBroadcast::new(10, 0, 10, Arc::new(DummyBroadcast::new()));
    let max = LimitedBroadcast::new(10, 1, 10, Arc::new(DummyBroadcast::new()));
    assert_lb_order(min, max);

    let min = LimitedBroadcast::new(10, 0, 11, Arc::new(DummyBroadcast::new()));
    let max = LimitedBroadcast::new(10, 0, 10, Arc::new(DummyBroadcast::new()));
    assert_lb_order(min, max);

    let min = LimitedBroadcast::new(11, 0, 10, Arc::new(DummyBroadcast::new()));
    let max = LimitedBroadcast::new(10, 0, 10, Arc::new(DummyBroadcast::new()));
    assert_lb_order(min, max);
}

fn assert_lb_order(min: LimitedBroadcast, max: LimitedBroadcast)
{
    let mut queue = BTreeSet::new();
    let shared_max = Arc::new(max);
    let shared_min = Arc::new(min);

    queue.insert(shared_max.clone());
    queue.insert(shared_min.clone());

    let actual_max = get_max(&queue);
    let actual_min = get_min(&queue);

    assert_eq!(actual_max, shared_max);
    assert_eq!(actual_min, shared_min);

    queue.remove(&shared_min);
    queue.remove(&shared_max);
    assert_eq!(queue.len(), 0);
}

#[test]
fn test_queue_broadcast()
{
    let mut queue = TransmitLimitedQueue::new(1);

    let broadcast1 = Arc::new(DummyBroadcast::new_with_msg(String::from("msg1")));
    let broadcast2 = Arc::new(DummyBroadcast::new_with_msg(String::from("msg2")));
    let broadcast3 = Arc::new(DummyBroadcast::new_with_msg(String::from("msg3")));

    queue.queue_broadcast(broadcast1.clone());
    queue.queue_broadcast(broadcast2.clone());
    queue.queue_broadcast(broadcast3.clone());

    assert_eq!(queue.len(), 3);

    let items = queue.collect();
    assert_eq!(items[0].get_message(), broadcast3.message());
    assert_eq!(items[1].get_message(), broadcast2.message());
    assert_eq!(items[2].get_message(), broadcast1.message());

    queue.queue_broadcast(Arc::new(DummyBroadcast::new_with_msg(String::from("msg1"))));

    assert_eq!(queue.len(), 3);

    let items = queue.collect();
    assert_eq!(items[0].get_message(), broadcast1.message());
    assert_eq!(items[1].get_message(), broadcast3.message());
    assert_eq!(items[2].get_message(), broadcast2.message());
}

#[test]
fn test_get_broadcast()
{
    let mut queue = TransmitLimitedQueue::new(1);
    let broadcast1 = Arc::new(DummyBroadcast::new_with_msg(String::from("msg1")));
    let broadcast2 = Arc::new(DummyBroadcast::new_with_msg(String::from("msg2")));
    let broadcast3 = Arc::new(DummyBroadcast::new_with_msg(String::from("msg3")));

    let val = queue.find_broadcasts(1, 3, 100);
    assert!(val.is_none());

    queue.queue_broadcast(broadcast1.clone());
    queue.queue_broadcast(broadcast2.clone());
    queue.queue_broadcast(broadcast3.clone());

    // No results if limit is zero
    let val = queue.find_broadcasts(1, 3, 0);
    assert!(val.is_some());
    assert_eq!(val.unwrap().len(), 0);

    let val = queue.find_broadcasts(1, 3, 20);
    assert!(val.is_some());
    assert_eq!(val.unwrap(), vec![broadcast3.message(), broadcast2.message()]);

    let val = queue.find_broadcasts(1, 3, 20);
    assert!(val.is_some());
    assert_eq!(val.unwrap(), vec![broadcast1.message(), broadcast3.message()]);

    let val = queue.find_broadcasts(1, 3, 20);
    assert!(val.is_some());
    assert_eq!(val.unwrap(), vec![broadcast2.message(), broadcast1.message()]);

    let val = queue.find_broadcasts(1, 3, 20);
    assert!(val.is_none());
}
