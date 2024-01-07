//! Corner-case tests.

use super::*;

const RPC_NOMSG: ReqType = 99;

/// Test if zero-sized requests & responses can be correctly handled.
#[test]
fn zero_sized() {
    let cli_port = next_port();
    let svr_port = next_port();

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    let handle = thread::spawn(move || {
        let mut nx = Nexus::new(("127.0.0.1", svr_port));
        nx.set_rpc_handler(RPC_NOMSG, |req| async move {
            let mut resp_buf = req.pre_resp_buf();
            resp_buf.set_len(0);
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
    let req_buf = rpc.alloc_msgbuf(0);
    let mut resp_buf = rpc.alloc_msgbuf(1);

    // Send request.
    let request = sess.request(RPC_NOMSG, &req_buf, &mut resp_buf);
    block_on(request);

    // Validation.
    assert_eq!(resp_buf.len(), 0);

    tx.send(()).unwrap();
    handle.join().unwrap();
}
