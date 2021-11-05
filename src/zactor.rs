use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

use crate::utils::{create_zmq_pipe, send_signal, wait_for_signal};
use core::result;
use log::{info, error};
use zmq::{Context, Message, Sendable, Socket};

pub trait ZHandler: Sync + Send {
    fn handle(&mut self, ctx: Arc<Context>, pipe: &Socket);
}

#[derive(Clone)]
pub struct ZActor {
    ctx: Arc<Context>,
    pipe: Arc<Mutex<Socket>>,
    shim_pipe: Arc<Mutex<Socket>>,
    handler: Arc<Mutex<dyn ZHandler>>,
    is_open: Arc<AtomicBool>,
}

impl ZActor {
    #[inline]
    pub fn new(ctx: Arc<Context>, handler: Arc<Mutex<dyn ZHandler>>) -> ZActor {
        let (pipe, shim_pipe) = create_zmq_pipe(ctx.clone());
        ZActor {
            ctx: ctx.clone(),
            pipe: Arc::new(Mutex::new(pipe)),
            shim_pipe: Arc::new(Mutex::new(shim_pipe)),
            handler: handler.clone(),
            is_open: Arc::new(AtomicBool::from(false)),
        }
    }

    pub fn run(&mut self) {
        let gaurd = self.shim_pipe.lock();
        match gaurd {
            Ok(shim_pipe) => {
                self.is_open.store(true, Ordering::Release);
                let mut guard = self.handler.lock().unwrap();
                guard.handle(self.ctx.clone(), &shim_pipe);
                shim_pipe.set_sndtimeo(0);
                send_signal(&shim_pipe);
            }
            Err(e) => {
                error!("Error while acquiring lock for shim pipe: {}", e)
            }
        }
    }

    pub fn start(&mut self) -> JoinHandle<()> {
        let mut actor = self.clone();

        thread::spawn(move || {
            actor.run();
        })
    }

    pub fn send<T>(&self, data: T, flags: i32) -> zmq::Result<()>
    where
        T: Sendable,
    {
        let pipe = self.pipe.lock().unwrap();
        return pipe.send(data, flags);
    }

    pub fn send_multipart<I, T>(&self, iter: I, flags: i32) -> zmq::Result<()>
    where
        I: IntoIterator<Item = T>,
        T: Into<Message>,
    {
        let pipe = self.pipe.lock().unwrap();
        return pipe.send_multipart(iter, flags);
    }

    pub fn recv_string(&self, flags: i32) -> zmq::Result<result::Result<String, Vec<u8>>> {
        let pipe = self.pipe.lock().unwrap();
        return pipe.recv_string(flags);
    }

    pub fn recv_multipart(&self, flags: i32) -> zmq::Result<Vec<Vec<u8>>> {
        let pipe = self.pipe.lock().unwrap();
        return pipe.recv_multipart(flags);
    }

    pub fn recv_bytes(&self, flags: i32) -> zmq::Result<Vec<u8>> {
        let pipe = self.pipe.lock().unwrap();
        return pipe.recv_bytes(flags);
    }
}

impl Drop for ZActor {
    fn drop(&mut self) {
        if !self.is_open.load(Ordering::Acquire) {
            return;
        }

        let gaurd = self.pipe.lock();
        match gaurd {
            Ok(pipe) => {
                pipe.set_sndtimeo(0);
                pipe.send("$TERM", 0);
                wait_for_signal(&pipe);
                info!("ZActor terminated successfully!");
            }
            Err(e) => {
                error!("Error while acquiring lock on pipe: {}", e)
            }
        }
    }
}
