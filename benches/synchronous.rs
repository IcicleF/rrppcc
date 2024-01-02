use criterion::{criterion_group, criterion_main, Criterion};

use futures::executor::block_on;
use rrppcc::{type_alias::ReqType, *};
use std::{ptr, sync::mpsc, thread};

pub fn criterion_benchmark(c: &mut Criterion) {
    const CLI_PORT: u16 = 31850;
    const SVR_PORT: u16 = 31851;

    const RPC_HELLO: ReqType = 42;
    const RPC_LEN: usize = 8;

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    let handle = thread::spawn(move || {
        let mut nx = Nexus::new(("127.0.0.1", SVR_PORT));
        nx.set_rpc_handler(RPC_HELLO, |req| async move {
            let mut resp_buf = req.pre_resp_buf();
            unsafe { ptr::write_bytes(resp_buf.as_ptr(), 1, RPC_LEN) };
            resp_buf.set_len(RPC_LEN);
            resp_buf
        });

        let rpc = Rpc::new(&nx, 2, "mlx5_0", 1);
        tx2.send(()).unwrap();
        while let Err(_) = rx.try_recv() {
            rpc.progress();
        }
    });

    let nx = Nexus::new(("127.0.0.1", CLI_PORT));
    let rpc = Rpc::new(&nx, 1, "mlx5_0", 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session(("127.0.0.1", SVR_PORT), 2);
    assert!(block_on(sess.connect()));

    // Prepare buffer.
    let req_buf = rpc.alloc_msgbuf(RPC_LEN);
    let mut resp_buf = rpc.alloc_msgbuf(RPC_LEN);

    // Benchmark synchronous pingpong-only RPC performance.
    c.bench_function("sync-pingpong", |b| {
        b.iter(|| {
            let request = sess.request(RPC_HELLO, &req_buf, &mut resp_buf);
            block_on(request);
        })
    });

    tx.send(()).unwrap();
    handle.join().unwrap();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
