mod event;

use std::future::Future;
use std::io::ErrorKind as IoErrorKind;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::{atomic::*, Arc};
use std::time::Duration;
use std::{array, thread};

use ahash::RandomState;
use dashmap::DashMap;
use quanta::Upkeep;
use rmp_serde as rmps;

pub(crate) use self::event::*;
use crate::handler::{ReqHandler, ReqHandlerFuture, RequestHandle};
use crate::msgbuf::MsgBuf;
use crate::type_alias::*;

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
///
/// The main purpose of this type is binding handlers to specific request
/// types. All handlers should be set before any [`Rpc`](`crate::Rpc`)s are
/// created.
///
/// # Background threads
///
/// On creation, the `Nexus` launches a session management thread
/// and a [`quanta::Upkeep`] thread. The former is for establishing
/// sessions between `Rpc`s, and the latter is for providing ms-precision
/// time for packet loss detection.
///
/// Logically, it can be meaningful to have multiple `Nexus`es in a process,
/// so if the `Upkeep` thread fails to start due to an existing one, the
/// `Nexus` will still be created. Be aware that if the existing `Upkeep`
/// thread is launched by you, then your configuration will affect the
/// packet loss detection of all `Rpc`s in the process.
pub struct Nexus {
    rpc_handlers: [Option<ReqHandler>; ReqType::MAX as usize + 1],

    sm: Arc<NexusSm>,
    sm_thread: Option<thread::JoinHandle<()>>,
    _upkeeper: Option<quanta::Handle>,
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

    /// Return `true` if the given request type is registered with a request handler.
    #[inline(always)]
    pub(crate) fn has_rpc_handler(&self, req_type: ReqType) -> bool {
        self.rpc_handlers[req_type as usize].is_some()
    }

    /// Call the registered request handler for the given request type.
    ///
    /// # Panics
    ///
    /// Panic if there is no such request handler.
    #[inline]
    pub(crate) fn call_rpc_handler(&self, req: RequestHandle) -> ReqHandlerFuture {
        let req_type = req.req_type();
        let handler = self.rpc_handlers[req_type as usize].as_ref().unwrap();
        handler(req)
    }
}

impl Nexus {
    /// Create a new Nexus instance.
    ///
    /// This also creates a [`quanta::Upkeep`] thread if there aren't any existing.
    /// `Rpc`s rely on the upkeeper to provide ms-precision time for packet loss detection.
    ///
    /// # Panics
    ///
    /// - Panic if the given URI cannot be resolved.
    /// - Panic if the upkeep thread cannot be spawned.
    pub fn new(uri: impl ToSocketAddrs) -> Arc<Self> {
        let uri = uri
            .to_socket_addrs()
            .expect("failed to resolve remote URI")
            .next()
            .expect("no such remote URI");

        // Bind to 0.0.0.0 or ::0, depending on the type of `uri`.
        let unspecified = match uri {
            SocketAddr::V4(_) => "0.0.0.0",
            SocketAddr::V6(_) => "::0",
        };
        let socket = UdpSocket::bind((unspecified, uri.port())).unwrap();

        const SOCKET_READ_TIMEOUT: Duration = Duration::from_millis(100);
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

        // Run a quanta upkeep thread that provides ms-precision time
        // for packet loss detection. This can fail due to an existing
        // upkeep thread, but failing to spawn the upkeep thread is
        // not tolerable.
        const UPKEEP_INTERVAL: Duration = Duration::from_millis(1);
        let upkeeper = Upkeep::new(UPKEEP_INTERVAL).start();

        if let Err(quanta::Error::FailedToSpawnUpkeepThread(ref e)) = upkeeper {
            panic!("failed to spawn clock upkeep thread: {}", e);
        }

        Arc::new(Self {
            rpc_handlers: array::from_fn(|_| None),
            sm,
            sm_thread: Some(sm_listener),
            _upkeeper: upkeeper.ok(),
        })
    }

    /// Get the URI that this Nexus is listening on.
    #[inline]
    pub fn uri(&self) -> SocketAddr {
        self.sm.uri
    }

    /// Set the handler for the given request type.
    /// This must be done before any [`Rpc`](`crate::Rpc`)s are created on this `Nexus`.
    ///
    /// The handler takes a request handle as argument, and must return a `MsgBuf` that
    /// belongs to the same `Rpc` calling the handler (otherwise, panic).
    ///
    /// # Panics
    ///
    /// Panic if there is already an `Rpc` created.
    pub fn set_rpc_handler<H, F>(self: &mut Arc<Self>, req_id: ReqType, handler: H)
    where
        H: Fn(RequestHandle) -> F + Send + Sync + 'static,
        F: Future<Output = MsgBuf> + Send + Sync + 'static,
    {
        let this =
            Arc::get_mut(self).expect("cannot set request handlers after `Rpc`s are created");
        this.rpc_handlers[req_id as usize] = Some(Box::new(move |req| Box::pin(handler(req))));
    }
}

impl Drop for Nexus {
    fn drop(&mut self) {
        self.sm.sm_should_stop.store(true, Ordering::SeqCst);
        self.sm_thread.take().unwrap().join().unwrap();
    }
}
