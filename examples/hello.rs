use futures::executor::block_on;
use rrppcc::{type_alias::*, *};
use std::{ptr, sync::mpsc, thread};

fn main() {
    const CLI_PORT: u16 = 31850;
    const SVR_PORT: u16 = 31851;
    const RPC_HELLO: ReqType = 42;
    const HELLO_WORLD: &str = "Hello, world!";

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    // Server thread.
    let handle = thread::spawn(move || {
        let mut nx = Nexus::new(("127.0.0.1", SVR_PORT));
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

    // Client thread.
    let nx = Nexus::new(("127.0.0.1", CLI_PORT));
    let rpc = Rpc::new(&nx, 1, "mlx5_0", 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session(("127.0.0.1", SVR_PORT), 2);
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
