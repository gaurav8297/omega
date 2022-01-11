use std::any::Any;
use std::sync::Arc;

use tokio::sync::broadcast::Sender;

pub trait Broadcast: Any
{
    fn get_name(&self) -> String;
    fn invalidates(&self, b: Arc<dyn Any + Send + Sync>) -> bool;
    fn message(&self) -> Arc<Vec<u8>>;
    fn finished(&self);
}

pub struct DummyBroadcast
{
    msg: Option<String>,
}

impl DummyBroadcast {
    pub fn new() -> DummyBroadcast
    {
        return DummyBroadcast { msg: None };
    }

    pub fn new_with_msg(m: String) -> DummyBroadcast
    {
        return DummyBroadcast { msg: Some(m) };
    }
}

impl Broadcast for DummyBroadcast {
    fn get_name(&self) -> String {
        return String::new();
    }

    fn invalidates(&self, b: Arc<dyn Any + Send + Sync>) -> bool {
        return match b.downcast::<DummyBroadcast>() {
            Ok(other) => {
                other.message() == self.message()
            }
            Err(_) => {
                false
            }
        };
    }

    fn message(&self) -> Arc<Vec<u8>> {
        return match self.msg.clone() {
            None => {
                Arc::new(vec![])
            }
            Some(m) => {
                Arc::new(m.into_bytes())
            }
        };
    }

    fn finished(&self) {
        return;
    }
}

pub struct OmegaBroadcast {
    node: String,
    msg: Arc<Vec<u8>>,
    notify: Option<Sender<()>>,
}

impl OmegaBroadcast {
    pub fn new(node: String,
               msg: Vec<u8>,
               notify: Option<Sender<()>>) -> OmegaBroadcast {
        return OmegaBroadcast {
            node,
            msg: Arc::new(msg),
            notify,
        };
    }
}

impl Broadcast for OmegaBroadcast {
    fn get_name(&self) -> String {
        return self.node.clone();
    }

    fn invalidates(&self, b: Arc<dyn Any + Send + Sync>) -> bool
    {
        return match b.downcast::<OmegaBroadcast>() {
            Ok(other) => {
                other.node == self.node
            }
            Err(_) => {
                false
            }
        };
    }

    fn message(&self) -> Arc<Vec<u8>>
    {
        return self.msg.clone();
    }

    fn finished(&self)
    {
        if self.notify.is_some() {
            self.notify.as_ref().unwrap().send(());
        }
    }
}
