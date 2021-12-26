use std::rc::Rc;
use std::collections::BTreeSet;
use std::cell::RefCell;

use crate::queue::{LimitedBroadcast, TransmitLimitedQueue, get_min, get_max};
use crate::broadcast::DummyBroadcast;

#[test]
fn test_limited_broadcast_order()
{
    let min = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast::new()),
        name: None
    };
    let max = LimitedBroadcast {
        transmit: 1,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast::new()),
        name: None
    };
    assert_lb_order(min, max);

    let min = LimitedBroadcast {
        transmit: 0,
        msg_len: 11,
        id: 10,
        broadcast: Rc::new(DummyBroadcast::new()),
        name: None
    };
    let max = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast::new()),
        name: None
    };
    assert_lb_order(min, max);

    let min = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 11,
        broadcast: Rc::new(DummyBroadcast::new()),
        name: None
    };
    let max = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast::new()),
        name: None
    };
    assert_lb_order(min, max);
}

fn assert_lb_order(min: LimitedBroadcast, max: LimitedBroadcast)
{
    let mut queue = BTreeSet::new();
    let shared_max = Rc::new(RefCell::new(max));
    let shared_min = Rc::new(RefCell::new(min));

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
    let mut queue = TransmitLimitedQueue::new(|| 1, 1);

    let msg1 = String::from("msg1");
    let msg2 = String::from("msg2");
    let msg3 = String::from("msg3");

    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg1.clone())));
    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg2.clone())));
    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg3.clone())));

    assert_eq!(queue.len(), 3);

    let items = queue.collect();
    assert_eq!(items[0].borrow().broadcast.message(), msg3.clone().into_bytes());
    assert_eq!(items[1].borrow().broadcast.message(), msg2.clone().into_bytes());
    assert_eq!(items[2].borrow().broadcast.message(), msg1.clone().into_bytes());

    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg1.clone())));

    assert_eq!(queue.len(), 3);

    let items = queue.collect();
    assert_eq!(items[0].borrow().broadcast.message(), msg1.into_bytes());
    assert_eq!(items[1].borrow().broadcast.message(), msg3.into_bytes());
    assert_eq!(items[2].borrow().broadcast.message(), msg2.into_bytes());
}

#[test]
fn test_get_broadcast()
{
    let mut queue = TransmitLimitedQueue::new(|| 1, 1);
    let msg1 = String::from("msg1");
    let msg2 = String::from("msg2");
    let msg3 = String::from("msg3");

    let val = queue.find_broadcasts(3, 100);
    assert!(val.is_none());

    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg1.clone())));
    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg2.clone())));
    queue.queue_broadcast(Rc::new(DummyBroadcast::new_with_msg(msg3.clone())));

    // No results if limit is zero
    let val = queue.find_broadcasts(3, 0);
    assert!(val.is_some());
    assert_eq!(val.unwrap().len(), 0);

    let val = queue.find_broadcasts(3, 20);
    assert!(val.is_some());
    assert_eq!(val.unwrap(), vec![msg3.clone().into_bytes(), msg2.clone().into_bytes()]);

    let val = queue.find_broadcasts(3, 20);
    assert!(val.is_some());
    assert_eq!(val.unwrap(), vec![msg1.clone().into_bytes(), msg3.clone().into_bytes()]);

    let val = queue.find_broadcasts(3, 20);
    assert!(val.is_some());
    assert_eq!(val.unwrap(), vec![msg2.clone().into_bytes(), msg1.clone().into_bytes()]);

    let val = queue.find_broadcasts(3, 20);
    assert!(val.is_none());
}