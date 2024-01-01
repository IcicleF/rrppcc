//! A simple RPC library that is designed following eRPC's ideas.

mod msgbuf;
mod nexus;
mod pkthdr;
mod request;
mod rpc;
mod session;
mod transport;
pub mod type_alias;
mod util;

pub use self::nexus::Nexus;
pub use self::rpc::Rpc;
pub use self::session::SessionHandle as Session;

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::*;
    use std::{sync::mpsc, thread};

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
}
