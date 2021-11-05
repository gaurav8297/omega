use log::{warn};
use rand::Rng;
use std::convert::TryInto;
use std::sync::Arc;
use zmq::{Context, Socket};
use std::mem::MaybeUninit;

pub fn create_zmq_pipe(ctx: Arc<Context>) -> (Socket, Socket) {
    let backend = ctx.socket(zmq::PAIR).unwrap();
    let frontend = ctx.socket(zmq::PAIR).unwrap();
    // TODO - Set the high water mark to some high value

    backend.set_linger(0);
    frontend.set_linger(0);
    let mut rng = rand::thread_rng();
    let mut endpoint = format!(
        "inproc://zactor-{}-{}",
        rng.gen_range(0, 0x10000),
        rng.gen_range(0, 0x10000)
    );
    loop {
        let result = frontend.bind(endpoint.as_str());
        if result.is_ok() {
            break;
        }
        endpoint = format!(
            "inproc://zactor-{}-{}",
            rng.gen_range(0, 0x10000),
            rng.gen_range(0, 0x10000)
        );
    }
    backend.connect(endpoint.as_str());
    return (frontend, backend);
}

pub fn send_signal(socket: &Socket) {
    let val: u64 = 0x7766554433221100 + 0;
    socket
        .send(val.to_be_bytes().to_vec(), 0)
        .expect("Error while sending message");
}

pub fn wait_for_signal(socket: &Socket) -> u64 {
    socket.set_rcvtimeo(5000);
    loop {
        let result = socket.recv_bytes(0);
        match result {
            Ok(msg) => {
                if msg.len() != 8 {
                    continue;
                }

                let signal = u64::from_be_bytes(msg.try_into().unwrap());
                if (signal & 0xFFFFFFFFFFFFFF00) == 0x7766554433221100 {
                    return signal & 255;
                }

                return 0;
            }
            Err(e) => {
                warn!("Error while receiving bytes: {}", e);
                break;
            }
        }
    }

    return 0;
}

pub fn convert_bytes_to_string(vec: Vec<u8>) -> String {
    return String::from_utf8(vec).expect("Unable to convert bytes to string");
}

pub fn convert_uninit_buffer_to_vec(input_buffer: [MaybeUninit<u8>; 255], buffer_size: usize) -> Vec<u8> {
    let mut out_buffer = vec![];

    for i in 0..buffer_size {
        let data = input_buffer[i];
        unsafe {
            out_buffer.push(data.assume_init());
        }
    }

    return out_buffer;
}

pub fn check_if_vec_equal(input: &Vec<u8>, other: &Vec<u8>) -> bool {
    return input.iter()
        .zip(other)
        .all(|(a, b)| *a == *b);
}