use criterion::{criterion_group, criterion_main, Criterion};

use futures::executor::block_on;
use rrppcc::{type_alias::ReqType, *};
use std::{ptr, sync::mpsc, thread};

const LOCALHOST: &'static str = "127.0.0.1";

pub fn benchmark_idle(c: &mut Criterion) {
    const PORT: u16 = 31850;

    let nx = Nexus::new((LOCALHOST, PORT));
    let rpc = Rpc::new(&nx, 1, "mlx5_0", 1);

    // Benchmark idle event-loop latency.
    c.bench_function("idle-eventloop", |b| {
        b.iter(|| {
            rpc.progress();
        })
    });
}

pub fn benchmark_sync(c: &mut Criterion) {
    const CLI_PORT: u16 = 31850;
    const SVR_PORT: u16 = 31851;

    const RPC_HELLO: ReqType = 42;
    const RPC_LEN: usize = 8;

    let (tx, rx) = mpsc::channel();
    let (tx2, rx2) = mpsc::channel();

    let handle = thread::spawn(move || {
        let mut nx = Nexus::new((LOCALHOST, SVR_PORT));
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

    let nx = Nexus::new((LOCALHOST, CLI_PORT));
    let rpc = Rpc::new(&nx, 1, "mlx5_0", 1);

    rx2.recv().unwrap();
    let sess = rpc.create_session((LOCALHOST, SVR_PORT), 2);
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

criterion_group!(benches, benchmark_idle, benchmark_sync);
criterion_main!(benches);
