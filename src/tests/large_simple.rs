//! Simple tests for large RPCs.

use super::*;

const HELLO_WORLD: &str = "hello, world!";
const RPC_HELLO: ReqType = 42;

/// Test a single RPC with large request.
#[test]
fn large_req() {
    let cli_port = next_port();
    let svr_port = next_port();

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    // Request length, intentionally some magic to verify it.
    const REQ_LEN: usize = 46382;
    let req_byte = rand::thread_rng().gen::<u8>();

    let handle = thread::spawn(move || {
        let mut nx = Nexus::new(("127.0.0.1", svr_port));
        nx.set_rpc_handler(RPC_HELLO, move |req| async move {
            assert_eq!(req.req_buf().len(), REQ_LEN);
            let payload = unsafe { req.req_buf().as_slice() };
            assert!(payload.iter().all(|&b| b == req_byte));

            let mut resp_buf = req.pre_resp_buf();
            unsafe {
                ptr::copy_nonoverlapping(HELLO_WORLD.as_ptr(), resp_buf.as_ptr(), HELLO_WORLD.len())
            };
            resp_buf.set_len(HELLO_WORLD.len());
            resp_buf
        });

        let rpc = Rpc::new(&nx, 2, NIC_NAME, 1);
        tx2.send(()).unwrap();
        while let Err(_) = rx.try_recv() {
            rpc.progress();
        }
    });

    let nx = Nexus::new(("127.0.0.1", cli_port));
    let rpc = Rpc::new(&nx, 1, NIC_NAME, 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session(("127.0.0.1", svr_port), 2);
    assert!(block_on(sess.connect()));

    // Prepare buffer.
    let req_buf = rpc.alloc_msgbuf(REQ_LEN);
    let mut resp_buf = rpc.alloc_msgbuf(16);

    // Send request.
    unsafe { ptr::write_bytes(req_buf.as_ptr(), req_byte, req_buf.len()) };
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

/// Test a single RPC with large response.
#[test]
fn large_resp() {
    let cli_port = next_port();
    let svr_port = next_port();

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    // Response length, intentionally some magic to verify it.
    const RESP_LEN: usize = 46382;
    let resp_byte = rand::thread_rng().gen::<u8>();

    let handle = thread::spawn(move || {
        let mut nx = Nexus::new(("127.0.0.1", svr_port));
        nx.set_rpc_handler(RPC_HELLO, move |req| async move {
            let resp_buf = req.rpc().alloc_msgbuf(RESP_LEN);
            unsafe { ptr::write_bytes(resp_buf.as_ptr(), resp_byte, RESP_LEN) };
            resp_buf
        });

        let rpc = Rpc::new(&nx, 2, NIC_NAME, 1);
        tx2.send(()).unwrap();
        while let Err(_) = rx.try_recv() {
            rpc.progress();
        }
    });

    let nx = Nexus::new(("127.0.0.1", cli_port));
    let rpc = Rpc::new(&nx, 1, NIC_NAME, 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session(("127.0.0.1", svr_port), 2);
    assert!(block_on(sess.connect()));

    // Prepare buffer.
    let req_buf = rpc.alloc_msgbuf(16);
    let mut resp_buf = rpc.alloc_msgbuf(50000);

    // Send request.
    unsafe { ptr::write_bytes(req_buf.as_ptr(), 0, req_buf.len()) };
    let request = sess.request(RPC_HELLO, &req_buf, &mut resp_buf);
    block_on(request);

    // Validation.
    assert!(resp_buf.len() == RESP_LEN);
    let payload = unsafe { resp_buf.as_slice() };
    assert!(payload.iter().all(|&b| b == resp_byte));

    tx.send(()).unwrap();
    handle.join().unwrap();
}
