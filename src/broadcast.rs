use std::rc::Rc;
use std::any::Any;
use tokio::sync::mpsc::UnboundedSender;

pub trait Broadcast: Any
{
    fn invalidates(&self, b: Box<dyn Any>) -> bool;
    fn message(&self) -> Vec<u8>;
    fn finished(&self);
}

pub struct DummyBroadcast
{
    msg: Option<String>
}

impl DummyBroadcast {
    pub fn new() -> DummyBroadcast
    {
        return DummyBroadcast{msg: None};
    }

    pub fn new_with_msg(m: String) -> DummyBroadcast
    {
        return DummyBroadcast{msg: Some(m)};
    }
}

impl Broadcast for DummyBroadcast {
    fn invalidates(&self, b: Box<dyn Any>) -> bool {
        return match b.downcast::<Rc<DummyBroadcast>>() {
            Ok(other) => {
                other.message() == self.message()
            }
            Err(_) => {
                false
            }
        }
    }

    fn message(&self) -> Vec<u8> {
        return match self.msg.clone() {
            None => {
                vec![]
            }
            Some(m) => {
                m.into_bytes()
            }
        };
    }

    fn finished(&self) {
        return;
    }
}

pub struct OmegaBroadcast {
    pub node: String,
    pub msg: Vec<u8>,
    pub notify: Option<UnboundedSender<()>>
}

impl Broadcast for OmegaBroadcast {
    fn invalidates(&self, b: Box<dyn Any>) -> bool
    {
        return match b.downcast::<Rc<OmegaBroadcast>>() {
            Ok(other) => {
                other.node == self.node
            }
            Err(_) => {
                false
            }
        }
    }

    fn message(&self) -> Vec<u8>
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
