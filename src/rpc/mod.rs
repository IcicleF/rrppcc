#![allow(private_bounds)]

mod pending;

use std::cell::RefCell;
use std::marker::PhantomPinned;
use std::net::ToSocketAddrs;
use std::net::UdpSocket;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::thread::{self, ThreadId};
use std::{cmp, mem, ptr};

use futures::future::FutureExt;
use futures::task::noop_waker_ref;
use rmp_serde as rmps;
use rrddmma::rdma::qp::QpEndpoint;

use self::pending::*;
use crate::util::{buddy::*, likely::*};
use crate::{msgbuf::*, nexus::*, pkthdr::*, request::*, session::*, transport::*, type_alias::*};

/// Interior-mutable state of an [`Rpc`] instance.
pub(crate) struct RpcInterior {
    /// Sessions.
    sessions: Vec<Session>,

    /// RDMA UD transport layer.
    tp: UdTransport,

    /// Pending request handlers.
    pending_handlers: Vec<PendingHandler>,

    /// Pending packet transmissions.
    pending_tx: Vec<TxItem>,

    /// Buffer allocator.
    /// This need to be the last one to drop.
    allocator: BuddyAllocator,
}

impl RpcInterior {
    /// Drain the pending TX queue, start their transmission on the transport.
    /// Also drain the send DMA queue if `drain_dma_queue` is set.
    fn drain_tx_batch(&mut self, drain_dma_queue: bool) {
        if !self.pending_tx.is_empty() {
            // SAFETY: items in `pending_tx` all points to valid peers and `MsgBuf`s,
            // which is guaranteed by `process_rx()` and `process_pending_handlers()`.
            unsafe { self.tp.tx_burst(&self.pending_tx, drain_dma_queue) };
            self.pending_tx.clear();
        }
    }
}

impl RpcInterior {
    /// Allocate a `MsgBuf` that can accommodate at least `len` bytes of
    /// application data. The allocated `MsgBuf` will have an initial length
    /// of `len`, but its content are uninitialized.
    #[inline]
    pub fn alloc_msgbuf(&mut self, len: usize) -> MsgBuf {
        // SAFETY: `self.allocator` is guaranteed to be valid.
        let buf = self
            .allocator
            .alloc(len + mem::size_of::<PacketHeader>(), &mut self.tp);
        MsgBuf::owned(buf, len)
    }
}

/// Thread-local RPC endpoint.
///
/// This type accepts a generic type that specifies the transport layer.
/// Available transports are provided in the [`crate::transport`] module.
///
/// This is the main type of this library.
pub struct Rpc {
    /// ID of this RPC instance.
    id: RpcId,
    /// Nexus this RPC is bound to.
    nexus: Pin<Arc<Nexus>>,
    /// The thread ID of the thread that created this RPC instance.
    pub(crate) thread_id: ThreadId,

    /// Session management packet sender.
    /// Use a independent socket, no need to delegate to the Nexus.
    pub(crate) sm_tx: UdpSocket,
    /// Session management event receiver.
    sm_rx: SmEventRx,

    /// Cached transport endpoint information.
    tp_ep: QpEndpoint,

    /// Interior-mutable state of this RPC.
    state: RefCell<RpcInterior>,

    /// A flag indicating whether this RPC is progressing.
    ///
    /// This flag exists because we have to leave `state` and `pending_tx` unborrowed
    /// when entering request handler contexts, but we still want to prevent reentrances
    /// of `progress()`.
    progressing: RefCell<()>,

    /// Pinning marker to prevent moving around.
    _pinned: PhantomPinned,
}

// Internal progress.Rx routines.
impl Rpc {
    /// Process a single-packet incoming request.
    /// Return `true` if the request handler returns immediately or not even called
    /// (which means the caller can release the Rx buffer).
    ///
    /// # Panics
    ///
    /// Panic if `self.state` is already borrowed.
    #[must_use = "you must the return value to decide whether to release the Rx buffer"]
    fn process_small_request(
        &self,
        sess_id: SessId,
        sslot_idx: usize,
        req_msgbuf: &MsgBuf,
    ) -> bool {
        // SAFETY: guaranteed not null and aligned.
        let hdr = unsafe { NonNull::new_unchecked(req_msgbuf.pkt_hdr()).as_mut() };
        debug_assert_eq!(
            hdr.pkt_type(),
            PktType::SmallReq,
            "packet type is not small-request"
        );

        // Check request type sanity.
        let req_type = hdr.req_type();
        if unlikely(!self.nexus.has_rpc_handler(req_type)) {
            log::debug!(
                "RPC {}: dropping received SmallRequest for unknown request type {:?}",
                self.id,
                req_type
            );
            return true;
        }

        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;
        let sess = &mut state.sessions[sess_id as usize];
        let sslot = &mut sess.slots[sslot_idx];

        // Check request index sanity.
        let req_idx = hdr.req_idx();
        if unlikely(req_idx <= sslot.req_idx) {
            // Simply drop outdated requests.
            if req_idx < sslot.req_idx {
                log::debug!(
                    "RPC {}: dropping received SmallRequest for outdated request (idx {}, ty {})",
                    self.id,
                    req_idx,
                    req_type
                );
            } else {
                // Here, this must be a retransmitted request.
                // RPCs should finish in a short time, so likely finished but packet lost.
                if likely(sslot.finished) {
                    // If the request is already finished, we need to retransmit the response.
                    log::debug!(
                        "RPC {}: retransmitting possibly-lost response for request (idx {}, ty {})",
                        self.id,
                        req_idx,
                        req_type
                    );

                    // Setup retransmission item.
                    state.pending_tx.push(TxItem {
                        peer: sess.peer.as_ref().unwrap(),
                        msgbuf: &sslot.resp,
                    });

                    // Need to drain the DMA queue.
                    // This is because a previous response might have already reached the client,
                    // i.e., this is an actually unnecessary retransmission. But if a future
                    // request is sent before this retransmission, the response can get corrupted.
                    //
                    // SAFETY: `item` points to a valid peer and `MsgBuf`.
                    state.drain_tx_batch(true);
                } else {
                    // If the request is simply not finished, we have nothing to do.
                    // Let the client wait.
                    log::debug!(
                        "RPC {}: client urging for response of request (idx {}, ty {}), but it is not finished yet",
                        self.id,
                        req_idx,
                        req_type
                    );
                }
            }
            return true;
        }

        // Now that this must be a valid *new* request.
        // Fill in the SSlot fields.
        sslot.finished = false;
        sslot.req_idx = req_idx;
        sslot.req_type = req_type;

        // Use `MsgBuf::clone_borrowed()` to keep the lkey, which is needed when releasing the buffer.
        sslot.req = req_msgbuf.clone_borrowed();

        // Construct the request. This will fetch the raw pointer of the current `Rpc` and `sslot`.
        let request = Request::new(self, sslot);
        drop(real_state);

        // Call the request handler with no `state` borrows.
        let mut resp_fut = self.nexus.call_rpc_handler(request);

        // Immediately poll the Future.
        // This will make synchronous handlers return, and push asynchronous handlers
        // into the first yield point.
        let mut cx = Context::from_waker(noop_waker_ref());
        let response = resp_fut.poll_unpin(&mut cx);

        // Now we can borrow `state` again.
        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;
        let sess = &mut state.sessions[sess_id as usize];
        let sslot = &mut sess.slots[sslot_idx];
        match response {
            Poll::Ready(resp) => {
                assert!(
                    resp.len() <= UdTransport::max_data_in_pkt(),
                    "large response unimplemented yet"
                );

                // Write the packet header of this MsgBuf.
                // SAFETY: `pkt_hdr` is not null and properly aligned, which is
                // checked by `PacketHeader::pkt_hdr()`.
                unsafe {
                    ptr::write(
                        resp.pkt_hdr(),
                        PacketHeader::new(
                            sslot.req_type,
                            resp.len() as _,
                            sess.peer_sess_id,
                            sslot.req_idx,
                            PktType::SmallResp,
                        ),
                    )
                };

                // Store the response buffer in the SSlot.
                // Previous response buffer will be buried at this point.
                sslot.resp = resp;
                sslot.finished = true;

                // Push the packet to the pending Tx queue.
                state.pending_tx.push(TxItem {
                    peer: sess.peer.as_ref().unwrap(),
                    msgbuf: &sslot.resp,
                });
                true
            }
            Poll::Pending => {
                state
                    .pending_handlers
                    .push(PendingHandler::new(sess_id, sslot_idx, resp_fut));
                false
            }
        }
    }

    /// Process a response packet.
    ///
    /// This method never calls request handlers, so it is safe to take a `&mut RpcInterior`
    /// as parameter.
    fn process_small_response(
        &self,
        state: &mut RpcInterior,
        sess_id: SessId,
        sslot_idx: usize,
        hdr: &PacketHeader,
        data: *mut u8,
    ) {
        debug_assert_eq!(
            hdr.pkt_type(),
            PktType::SmallResp,
            "packet type is not small-response"
        );

        let sess = &mut state.sessions[sess_id as usize];
        let sslot = &mut sess.slots[sslot_idx];

        // Check packet metadata sanity.
        if unlikely(hdr.req_type() != sslot.req_type || hdr.req_idx() != sslot.req_idx) {
            assert!(
                hdr.req_idx() < sslot.req_idx,
                "response for future request is impossible"
            );
            log::debug!(
                "RPC {}: dropping received SmallResponse for expired request (idx {}, ty {})",
                self.id,
                hdr.req_idx(),
                hdr.req_type()
            );
            return;
        }

        // Truncate response if needed.
        let len = cmp::min(sslot.resp.capacity(), hdr.data_len() as usize);
        sslot.resp.set_len(len);

        // SAFETY: source is guaranteed to be valid, destination length checked, may not overlap.
        unsafe { ptr::copy_nonoverlapping(data, sslot.resp.as_ptr(), len) };
        sslot.finished = true;
    }
}

// Internal progress routines.
impl Rpc {
    /// Process received session management events.
    /// This method works purely on the control plane.
    fn process_sm_events(&self) {
        // Should not panic, as we have checked reentrancy in `progress()`.
        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;

        while let Some(event) = self.sm_rx.recv() {
            log::trace!("RPC {}: received {:#?}", self.id, event);
            debug_assert_eq!(event.dst_rpc_id, self.id, "bad SM event dispatch");

            match event.details {
                SmEventDetails::ConnectRequest {
                    cli_uri: uri,
                    cli_ep,
                    cli_sess_id,
                } => {
                    // Endpoint deserialization error means some severe control-plane network error
                    // or a bug in the peer. Although we can ignore, the log level should be raised
                    // to `error` to draw attention.
                    let Ok(cli_ep) = rmps::from_slice(&cli_ep) else {
                        log::error!(
                            "RPC {}: ignoring ConnectAcknowledge for session {} with invalid server endpoint",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    };
                    let peer = state.tp.create_peer(cli_ep);
                    let svr_ep = rmps::to_vec(&state.tp.endpoint()).unwrap();

                    // Setup a initially connected session.
                    let mut sess = Session::new(state, SessionRole::Server);
                    sess.peer_uri = uri;
                    sess.peer_rpc_id = event.src_rpc_id;
                    sess.peer_sess_id = cli_sess_id;
                    sess.peer = Some(peer);
                    sess.connected = Some(true);

                    let svr_sess_id = state.sessions.len() as SessId;
                    state.sessions.push(sess);

                    // Send ConnectAcknowledge.
                    let ack = SmEvent {
                        src_rpc_id: self.id,
                        dst_rpc_id: event.src_rpc_id,
                        details: SmEventDetails::ConnectAcknowledge {
                            cli_sess_id,
                            svr_ep,
                            svr_sess_id,
                        },
                    };
                    let ack_buf = rmps::to_vec(&ack).unwrap();
                    self.sm_tx
                        .send_to(&ack_buf, uri)
                        .expect("failed to send ConnectAcknowledge");
                }
                SmEventDetails::ConnectAcknowledge {
                    cli_sess_id,
                    svr_ep,
                    svr_sess_id,
                } => {
                    if unlikely(state.sessions.len() <= cli_sess_id as usize) {
                        log::debug!(
                            "RPC {}: ignoring ConnectAcknowledge for non-existent session {}",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    }

                    let sess = &mut state.sessions[cli_sess_id as usize];

                    if unlikely(
                        sess.is_connected()
                            || !sess.is_client()
                            || sess.peer_rpc_id != event.src_rpc_id,
                    ) {
                        log::debug!(
                            "RPC {}: ignoring ConnectAcknowledge for client session {}, because of (connected, role, peer_rpc_id) mismatch: expected {:?}, found {:?}",
                            self.id,
                            cli_sess_id,
                            (false, SessionRole::Client, sess.peer_rpc_id),
                            (sess.is_connected(), SessionRole::Server, event.src_rpc_id),
                        );
                        continue;
                    }

                    // Endpoint deserialization error means some severe control-plane network error
                    // or a bug in the peer. Although we can ignore, the log level should be raised
                    // to `error` to draw attention.
                    let Ok(svr_ep) = rmps::from_slice(&svr_ep) else {
                        log::error!(
                            "RPC {}: ignoring ConnectAcknowledge for session {} with invalid server endpoint",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    };
                    let peer = state.tp.create_peer(svr_ep);

                    sess.peer_sess_id = svr_sess_id;
                    sess.peer = Some(peer);
                    sess.connected = Some(true);
                }
                SmEventDetails::ConnectRefuse {
                    cli_sess_id,
                    reason,
                } => {
                    if state.sessions.len() <= cli_sess_id as usize {
                        log::debug!(
                            "RPC {}: ignoring ConnectRefuse for non-existent session {}",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    }

                    log::debug!(
                        "RPC {}: session {} is refused by remote peer, due to {:?}",
                        self.id,
                        cli_sess_id,
                        reason
                    );
                    state.sessions[event.dst_rpc_id as usize].peer.take();
                }
                SmEventDetails::Disconnect => unimplemented!(),
            }
        }
    }

    /// Process received datapath packets.
    fn process_rx(&self) {
        // Should not panic, as we have checked reentrancy in `progress()`.
        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;

        // Do RX burst.
        let n = state.tp.rx_burst();

        // Process the received packets.
        let mut rx_bufs_to_release = Vec::with_capacity(n);

        // Process the received packets.
        // Perform sanity check, and handle received responses.
        // Requests are handled later since they will call request handler functions
        // and require `state` unborrowed.
        let mut rx_requests = Vec::with_capacity(n);
        for _ in 0..n {
            let item = state.tp.rx_next().expect("failed to fetch received packet");

            // SAFETY: guaranteed not null and aligned.
            let hdr = unsafe { NonNull::new_unchecked(item.pkt_hdr()).as_mut() };
            let sess_id = hdr.dst_sess_id();
            if unlikely(state.sessions.len() as u32 <= sess_id) {
                log::debug!(
                    "RPC {}: dropping received data-plane packet for non-existent session {}",
                    self.id,
                    sess_id
                );
            }

            // Perform session sanity check.
            let sess = &state.sessions[sess_id as usize];
            if unlikely(!sess.is_connected()) {
                log::debug!(
                    "RPC {}: dropping received data-plane packet for non-connected session {}",
                    self.id,
                    sess_id
                );
            }
            match hdr.pkt_type() {
                PktType::SmallReq | PktType::LargeReqCtrl => {
                    if unlikely(sess.is_client()) {
                        log::debug!(
                            "RPC {}: dropping received {:?} for client session {}",
                            self.id,
                            hdr.pkt_type(),
                            sess_id
                        );
                        continue;
                    }
                }
                PktType::SmallResp | PktType::LargeRespCtrl => {
                    if unlikely(!sess.is_client()) {
                        log::debug!(
                            "RPC {}: dropping received {:?} for server session {}",
                            self.id,
                            hdr.pkt_type(),
                            sess_id
                        );
                        continue;
                    }
                }
            }

            // Perform packet sanity check.
            if matches!(hdr.pkt_type(), PktType::SmallReq | PktType::SmallResp)
                && unlikely(hdr.data_len() > UdTransport::max_data_in_pkt() as u32)
            {
                log::debug!(
                    "RPC {}: dropping received {:?} with too large data length {}",
                    self.id,
                    hdr.pkt_type(),
                    hdr.data_len()
                );
                continue;
            }

            // Trigger packet processing logic.
            match hdr.pkt_type() {
                PktType::SmallReq => rx_requests.push(item),
                PktType::LargeReqCtrl => todo!("long request"),
                PktType::SmallResp => {
                    let sslot_idx = hdr.req_idx() as usize % ACTIVE_REQ_WINDOW;
                    self.process_small_response(state, sess_id, sslot_idx, hdr, item.as_ptr());
                    rx_bufs_to_release.push(item);
                }
                PktType::LargeRespCtrl => todo!("long response"),
            }
        }

        // Drop the mutable borrow ...
        drop(real_state);

        // ... so that we can start to handle requests.
        for item in rx_requests {
            // SAFETY: guaranteed not null and aligned.
            let hdr = unsafe { NonNull::new_unchecked(item.pkt_hdr()).as_mut() };
            let sess_id = hdr.dst_sess_id();
            let sslot_idx = hdr.req_idx() as usize % ACTIVE_REQ_WINDOW;

            if self.process_small_request(sess_id, sslot_idx, &item) {
                rx_bufs_to_release.push(item);
            }
        }

        // Release the Rx buffers to the transport layer.
        // SAFETY: `rx_bufs_to_release` contains valid Rx buffers, and each buffer
        // is only released once (which is this release).
        let mut real_state = self.state.borrow_mut();
        for item in rx_bufs_to_release {
            unsafe { real_state.tp.rx_release(&item) };
        }
    }

    /// Transmit pending packets.
    fn process_tx(&self) {
        // Should not panic, as we have checked reentrancy in `progress()`.
        let mut state = self.state.borrow_mut();
        state.drain_tx_batch(false);
    }

    /// Poll pending request handlers.
    fn process_pending_handlers(&self) {
        // Should not panic, as we have checked reentrancy in `progress()`.
        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;

        // Fetch the pending handlers into a local context.
        // This is to prevent borrowing `state` while we are polling the handlers.
        let mut pending_handlers = Vec::new();
        mem::swap(&mut state.pending_handlers, &mut pending_handlers);

        // Drop the mutable borrow ...
        drop(real_state);

        // ... so that we can start to poll the handlers.
        let mut finished_handlers = Vec::with_capacity(pending_handlers.len());
        let mut cx = Context::from_waker(noop_waker_ref());
        pending_handlers.retain_mut(|resp_fut| match resp_fut.poll_unpin(&mut cx) {
            Poll::Ready(resp) => {
                finished_handlers.push((resp_fut.sess_id, resp_fut.sslot_idx, resp));
                false
            }
            Poll::Pending => true,
        });

        // Now we can borrow `state` again.
        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;

        // First, put the pending handlers back to `state`.
        debug_assert!(
            state.pending_handlers.is_empty(),
            "there should be no reentrant `progress()`, why are there new pending handlers?"
        );
        mem::swap(&mut state.pending_handlers, &mut pending_handlers);

        // Then, transmit the responses.
        for (sess_id, sslot_idx, resp) in finished_handlers {
            assert!(
                resp.len() <= UdTransport::max_data_in_pkt(),
                "large response unimplemented yet"
            );

            let sess = &mut state.sessions[sess_id as usize];
            let sslot = &mut sess.slots[sslot_idx];

            // Release the request buffer.
            // SAFETY: this buffer is not released before because
            // - `process_small_request()` returns `false`, preventing it from getting
            //   released in `process_rx()`;
            // - previous polls to the handler future gives `Pending`, not doing anything
            //   to this buffer.
            unsafe { state.tp.rx_release(&sslot.req) };

            // Write the packet header of this MsgBuf.
            // SAFETY: `pkt_hdr` is not null and properly aligned, which is
            // checked by `PacketHeader::pkt_hdr()`.
            unsafe {
                ptr::write(
                    resp.pkt_hdr(),
                    PacketHeader::new(
                        sslot.req_type,
                        resp.len() as _,
                        sess.peer_sess_id,
                        sslot.req_idx,
                        PktType::SmallResp,
                    ),
                )
            };

            // Store the response buffer in the SSlot.
            // Previous response buffer will be buried at this point.
            sslot.resp = resp;
            sslot.finished = true;

            // Push the packet to the pending TX queue.
            state.pending_tx.push(TxItem {
                peer: sess.peer.as_ref().unwrap(),
                msgbuf: &sslot.resp,
            });
        }
    }
}

// Crate-internal API.
impl Rpc {
    /// Return `true` if the session of the given ID exists and is connected.
    ///
    /// # Panics
    ///
    /// - Panic if the session ID is invalid.
    /// - Panic if called when `self.state` is already borrowed.
    #[inline]
    pub(crate) fn session_connection_state(&self, sess_id: SessId) -> Option<bool> {
        self.state.borrow().sessions[sess_id as usize].connected
    }

    /// Mark a session as connecting in progress.
    ///
    /// # Panics
    ///
    /// - Panic if the session ID is invalid.
    /// - Panic if the session is already connected.
    #[inline]
    pub(crate) fn mark_session_connecting(&self, sess_id: SessId) {
        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;

        let sess = &mut state.sessions[sess_id as usize];
        if let Some(true) = sess.connected {
            panic!("session {} is already connected", sess_id);
        }
        sess.connected = None;
    }
}

// Public API.
impl Rpc {
    /// Create a new `Rpc` instance that is bound to a [`Nexus`] with a certain
    /// ID and the current thread. Will operate on the specified port of the given
    /// device. The given ID must be unique among all RPCs in the same Nexus.
    ///
    /// The created `Rpc` instance is pinned on heap to prevent moving around.
    /// Otherwise, it can invalidate pointers recorded in request handles.
    ///
    /// # Panics
    ///
    /// - Panic if the given ID is already used.
    /// - Panic if there is no such device or no such port.
    pub fn new(nexus: &Pin<Arc<Nexus>>, id: RpcId, nic: &str, phy_port: u8) -> Pin<Box<Self>> {
        const PREALLOC_SIZE: usize = 64;

        // Create the SM event channel first, so that it will immediately
        // panic if the given ID is already used.
        let sm_rx = nexus.register_event_channel(id);
        let tp = UdTransport::new(nic, phy_port);
        Box::pin(Self {
            id,
            nexus: nexus.clone(),
            thread_id: thread::current().id(),

            sm_tx: UdpSocket::bind("0.0.0.0:0").unwrap(),
            sm_rx,
            tp_ep: tp.endpoint(),
            state: RefCell::new(RpcInterior {
                sessions: Vec::new(),
                allocator: BuddyAllocator::new(),
                tp,
                pending_handlers: Vec::with_capacity(PREALLOC_SIZE),
                pending_tx: Vec::with_capacity(PREALLOC_SIZE),
            }),
            progressing: RefCell::new(()),
            _pinned: PhantomPinned,
        })
    }

    /// Return the ID of this `Rpc` instance.
    #[inline(always)]
    pub fn id(&self) -> RpcId {
        self.id
    }

    /// Return a reference to the `Nexus` bound to.
    #[inline(always)]
    pub fn nexus(&self) -> &Nexus {
        &self.nexus
    }

    /// Return the RDMA UD QP endpoint information of this `Rpc` instance.
    #[inline(always)]
    pub fn datagram_endpoint(&self) -> QpEndpoint {
        self.tp_ep
    }

    /// Create a client session that connects to a remote `Rpc` instance.
    ///
    /// The created session is not connected by default, and it will spend no
    /// efforts on establishing the connection until you call
    /// [`Session::connect()`](SessionHandle::connect) on it.
    ///
    /// The returned object does not have ownership of the session, but is purely
    /// a handle to it. You may first remember the session ID and drop the handle.
    /// When needed, you can acquire a new handle by calling [`Rpc::get_session()`].
    pub fn create_session(
        &self,
        remote_uri: impl ToSocketAddrs,
        remote_rpc_id: RpcId,
    ) -> SessionHandle {
        // Get the URI first so that we can panic early.
        let remote_uri = remote_uri
            .to_socket_addrs()
            .expect("failed to resolve remote URI")
            .next()
            .expect("no such remote URI");

        let mut real_state = self.state.borrow_mut();
        let state: &mut RpcInterior = &mut real_state;

        assert!(
            state.sessions.len() < SessId::MAX as usize,
            "too many sessions"
        );

        // Initialize the session with remote peer information.
        // `peer` and `peer_sess_id` will be filled in when the connection is established.
        let sess_id = state.sessions.len() as SessId;
        let mut sess = Session::new(state, SessionRole::Client);
        sess.peer_uri = remote_uri;
        sess.peer_rpc_id = remote_rpc_id;
        state.sessions.push(sess);

        SessionHandle::new(self, sess_id, remote_uri, remote_rpc_id)
    }

    /// Return a handle to a session of the given ID.
    pub fn get_session(&self, sess_id: SessId) -> Option<SessionHandle> {
        let state = self.state.borrow();
        if state.sessions.len() <= sess_id as usize {
            return None;
        }

        let sess = &state.sessions[sess_id as usize];
        Some(SessionHandle::new(
            self,
            sess_id,
            sess.peer_uri,
            sess.peer_rpc_id,
        ))
    }

    /// Allocate a `MsgBuf` that can accommodate at least `len` bytes of
    /// application data. The allocated `MsgBuf` will have an initial length
    /// of `len`, but its content are uninitialized.
    ///
    /// `MsgBuf`s allocated by one `Rpc` instance can only be used in the same
    /// `Rpc` instance. This is because different `Rpc`s have different RDMA
    /// device contexts, and the `MsgBuf`'s lkey is bound to the device context.
    ///
    /// # Panics
    ///
    /// Panic if buffer allocation fails.
    #[inline]
    pub fn alloc_msgbuf(&self, len: usize) -> MsgBuf {
        self.state.borrow_mut().alloc_msgbuf(len)
    }

    /// Run an iteration of event loop to make progress.
    /// Performs tasks including:
    /// - handling session connection requests and responses,
    /// - hanlding datapath requests and responses,
    /// - scheduling and (re)transmitting datapath packets.
    #[inline]
    pub fn progress(&self) {
        // Prevent reentrance of this method.
        let Ok(_flag) = self.progressing.try_borrow_mut() else {
            return;
        };

        // Deal with session management events, which should be rare.
        if unlikely(!self.sm_rx.is_empty()) {
            self.process_sm_events();
        }

        // Ordering:
        // - Rx should be processed after polling pending handlers,
        //   or newcomers will be polled twice, the second time being
        //   largely meaningless.
        // - Tx should be processed after polling pending handlers and
        //   Rx, because they may generate response packets to be sent
        //   in `pending_tx`.
        self.process_pending_handlers();
        self.process_rx();
        self.process_tx();
    }
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.nexus.destroy_event_channel(self.id);
    }
}
