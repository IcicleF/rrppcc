mod event;

use std::future::Future;
use std::io::ErrorKind as IoErrorKind;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::pin::Pin;
use std::sync::{atomic::*, Arc};
use std::{array, thread, time};

use ahash::RandomState;
use dashmap::DashMap;
use rmp_serde as rmps;

pub(crate) use self::event::*;
use crate::msgbuf::MsgBuf;
use crate::request::{ReqHandler, ReqHandlerFuture, Request};
use crate::type_alias::*;

static NEXUS_CREATED: AtomicBool = AtomicBool::new(false);

/// Session management part of [`Nexus`].
struct NexusSm {
    uri: SocketAddr,
    sm_evt_tx: DashMap<RpcId, SmEventTx, RandomState>,
    sm_should_stop: AtomicBool,
}

impl NexusSm {
    /// Listen on the given socket for SM events.
    fn listen(self: Arc<Self>, socket: UdpSocket) {
        const EVENT_MSG_SIZE_LIMIT: usize = 4 << 10; // 4 KiB
        let mut buf = [0u8; EVENT_MSG_SIZE_LIMIT];
        while !self.sm_should_stop.load(Ordering::Relaxed) {
            let (amt, src) = match socket.recv_from(&mut buf) {
                Ok(v) => v,
                Err(ref e)
                    if matches!(e.kind(), IoErrorKind::WouldBlock | IoErrorKind::TimedOut) =>
                {
                    continue
                }
                Err(e) => panic!("failed to receive UDP packet: {}", e),
            };
            let Ok(evt) = rmps::from_slice::<SmEvent>(&buf[..amt]) else {
                // UDP is unreliable, so we just ignore the packet if it is malformed.
                log::debug!("Nexus SM: ignoring malformed event from {}", src);
                continue;
            };

            let dst = evt.dst_rpc_id;
            match self.sm_evt_tx.get(&dst) {
                Some(tx) => tx.send(evt),
                None => log::debug!("Nexus SM: ignoring event to non-existent RPC {}", dst),
            };
        }
    }
}

/// A per-process singleton used for library initialization.
pub struct Nexus {
    rpc_handlers: [Option<ReqHandler>; ReqType::MAX as usize + 1],

    sm: Arc<NexusSm>,
    sm_thread: Option<thread::JoinHandle<()>>,
}

impl Nexus {
    /// Create an event channel for the given RPC ID.
    ///
    /// # Panics
    ///
    /// Panic if the RPC ID is already used.
    pub(crate) fn register_event_channel(&self, rpc_id: RpcId) -> SmEventRx {
        let (tx, rx) = sm_event_channel();
        assert!(self.sm.sm_evt_tx.insert(rpc_id, tx).is_none());
        rx
    }

    /// Destroy the event channel for the given RPC ID.
    pub(crate) fn destroy_event_channel(&self, rpc_id: RpcId) {
        self.sm.sm_evt_tx.remove(&rpc_id);
    }

    /// Return `true` if the given request type is registered with a RPC handler.
    #[inline(always)]
    pub(crate) fn has_rpc_handler(&self, req_type: ReqType) -> bool {
        self.rpc_handlers[req_type as usize].is_some()
    }

    /// Call the registered RPC handler for the given request type.
    ///
    /// # Panics
    ///
    /// Panic if there is no such RPC handler.
    #[inline]
    pub(crate) fn call_rpc_handler(&self, req: Request) -> ReqHandlerFuture {
        let req_type = req.req_type();
        let handler = self.rpc_handlers[req_type as usize].as_ref().unwrap();
        handler(req)
    }
}

impl Nexus {
    /// Create a new Nexus instance.
    ///
    /// # Panics
    ///
    /// - Panic if a Nexus instance has already been created and not dropped.
    /// - Panic if the given URI cannot be resolved.
    pub fn new(uri: impl ToSocketAddrs) -> Pin<Arc<Self>> {
        assert!(!NEXUS_CREATED.swap(true, Ordering::SeqCst));

        let uri = uri.to_socket_addrs().unwrap().next().unwrap();
        let socket = UdpSocket::bind(uri).unwrap();

        const SOCKET_READ_TIMEOUT: time::Duration = time::Duration::from_millis(100);
        socket.set_read_timeout(Some(SOCKET_READ_TIMEOUT)).unwrap();

        // Make the session manager.
        let sm = Arc::new(NexusSm {
            uri,
            sm_evt_tx: DashMap::with_capacity_and_hasher(256, RandomState::new()),
            sm_should_stop: AtomicBool::new(false),
        });
        let sm_listener = {
            let sm = sm.clone();
            thread::spawn(move || sm.listen(socket))
        };
        Arc::pin(Self {
            rpc_handlers: array::from_fn(|_| None),
            sm,
            sm_thread: Some(sm_listener),
        })
    }

    /// Get the URI that this Nexus is listening on.
    #[inline]
    pub fn uri(&self) -> SocketAddr {
        self.sm.uri
    }

    /// Set the RPC handler for the given request ID.
    pub fn set_rpc_handler<H, F>(&mut self, req_id: ReqType, handler: H) -> &mut Self
    where
        H: Fn(Request) -> F + Send + Sync + 'static,
        F: Future<Output = MsgBuf> + Send + Sync + 'static,
    {
        self.rpc_handlers[req_id as usize] = Some(Box::new(move |req| Box::pin(handler(req))));
        self
    }
}

impl Drop for Nexus {
    fn drop(&mut self) {
        self.sm.sm_should_stop.store(true, Ordering::SeqCst);
        self.sm_thread.take().unwrap().join().unwrap();

        NEXUS_CREATED.store(false, Ordering::SeqCst);
    }
}
