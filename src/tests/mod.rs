#![allow(unused_imports)]

use super::{type_alias::*, *};
use std::{
    ptr,
    sync::{atomic::*, *},
    thread,
};

use futures::executor::block_on;
use futures::future::join_all;
use simple_logger::SimpleLogger;

#[test]
fn create_rpcs() {
    let nexus = Nexus::new("127.0.0.1:31850");
    let handles = (1..=16).map(|i| {
        let nexus = nexus.clone();
        thread::spawn(move || {
            let _ = Rpc::new(&nexus, i, "mlx5_0", 1);
        })
    });
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn connect_rpcs() {
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let nx = Nexus::new("127.0.0.1:31852");
        let rpc = Rpc::new(&nx, 3, "mlx5_0", 1);
        while let Err(_) = rx.try_recv() {
            rpc.progress();
        }
    });

    let nx = Nexus::new("127.0.0.1:31851");
    let r1 = Rpc::new(&nx, 1, "mlx5_0", 1);
    let r2 = Rpc::new(&nx, 2, "mlx5_0", 1);

    (0..10).for_each(|i| {
        let sess = r1.create_session("127.0.0.1:31852", 3);
        assert_eq!(sess.id(), i);
        assert_eq!(sess.is_connected(), false);

        assert!(block_on(async { sess.connect().await }));
        assert!(sess.is_connected());
    });
    (0..10).for_each(|i| {
        let sess = r2.create_session("127.0.0.1:31852", 3);
        assert_eq!(sess.id(), i);
        assert_eq!(sess.is_connected(), false);

        assert!(block_on(async { sess.connect().await }));
        assert!(sess.is_connected());
    });

    tx.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
fn single_request() {
    const HELLO_WORLD: &str = "hello, world!";
    const RPC_HELLO: ReqType = 42;

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut nx = Nexus::new("127.0.0.1:31854");
        nx.set_rpc_handler(RPC_HELLO, |req| async move {
            let mut resp_buf = req.pre_resp_buf();
            unsafe {
                ptr::copy_nonoverlapping(HELLO_WORLD.as_ptr(), resp_buf.as_ptr(), HELLO_WORLD.len())
            };
            resp_buf.set_len(HELLO_WORLD.len());
            resp_buf
        });

        let rpc = Rpc::new(&nx, 2, "mlx5_0", 1);
        tx2.send(()).unwrap();
        while let Err(_) = rx.try_recv() {
            rpc.progress();
        }
    });

    let nx = Nexus::new("127.0.0.1:31853");
    let rpc = Rpc::new(&nx, 1, "mlx5_0", 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session("127.0.0.1:31854", 2);
    assert!(block_on(sess.connect()));

    // Prepare buffer.
    let req_buf = rpc.alloc_msgbuf(16);
    let mut resp_buf = rpc.alloc_msgbuf(16);

    // Send request.
    let request = sess.request(RPC_HELLO, &req_buf, &mut resp_buf);
    block_on(request);

    // Validation.
    let payload = {
        let mut payload = Vec::with_capacity(resp_buf.len());
        unsafe {
            ptr::copy_nonoverlapping(resp_buf.as_ptr(), payload.as_mut_ptr(), resp_buf.len());
            payload.set_len(resp_buf.len());
        }
        String::from_utf8(payload).unwrap()
    };
    assert_eq!(payload, HELLO_WORLD);

    tx.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
fn multiple_requests() {
    const HELLO_WORLD: &str = "hello, world!";
    const RPC_HELLO: ReqType = 42;

    // SimpleLogger::new().init().unwrap();

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut nx = Nexus::new("127.0.0.1:31856");
        nx.set_rpc_handler(RPC_HELLO, |req| async move {
            let mut resp_buf = req.pre_resp_buf();
            assert!(resp_buf.len() == 4080);
            unsafe {
                ptr::copy_nonoverlapping(HELLO_WORLD.as_ptr(), resp_buf.as_ptr(), HELLO_WORLD.len())
            };
            resp_buf.set_len(HELLO_WORLD.len());
            resp_buf
        });

        let rpc = Rpc::new(&nx, 2, "mlx5_0", 1);
        tx2.send(()).unwrap();
        while let Err(_) = rx.try_recv() {
            rpc.progress();
        }
    });

    let nx = Nexus::new("127.0.0.1:31855");
    let rpc = Rpc::new(&nx, 1, "mlx5_0", 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session("127.0.0.1:31856", 2);
    assert!(block_on(sess.connect()));

    // Prepare buffer.
    let req_buf = rpc.alloc_msgbuf(16);
    let mut resp_buf = rpc.alloc_msgbuf(16);

    // Send request.
    for _ in 0..500000 {
        let request = sess.request(RPC_HELLO, &req_buf, &mut resp_buf);
        block_on(request);

        // Validation.
        let payload = {
            let mut payload = Vec::with_capacity(resp_buf.len());
            unsafe {
                ptr::copy_nonoverlapping(resp_buf.as_ptr(), payload.as_mut_ptr(), resp_buf.len());
                payload.set_len(resp_buf.len());
            }
            String::from_utf8(payload).unwrap()
        };
        assert_eq!(payload, HELLO_WORLD);
    }

    tx.send(()).unwrap();
    handle.join().unwrap();
}
