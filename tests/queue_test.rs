use std::collections::BTreeSet;
use omega::queue::{LimitedBroadcast, DummyBroadcast, get_max, get_min};
use std::rc::Rc;
use std::cell::RefCell;

#[test]
fn test_limited_broadcast_order()
{
    let min = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast{}),
        name: None
    };
    let max = LimitedBroadcast {
        transmit: 1,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast{}),
        name: None
    };
    assert_lb_order(min, max);

    let min = LimitedBroadcast {
        transmit: 0,
        msg_len: 11,
        id: 10,
        broadcast: Rc::new(DummyBroadcast{}),
        name: None
    };
    let max = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast{}),
        name: None
    };
    assert_lb_order(min, max);

    let min = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 11,
        broadcast: Rc::new(DummyBroadcast{}),
        name: None
    };
    let max = LimitedBroadcast {
        transmit: 0,
        msg_len: 10,
        id: 10,
        broadcast: Rc::new(DummyBroadcast{}),
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
}
