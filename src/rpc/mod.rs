#![allow(private_bounds)]

mod pending;

use std::cell::RefCell;
use std::net::UdpSocket;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{cmp, mem, ptr};

use futures::future::FutureExt;
use futures::task::noop_waker_ref;
use rmp_serde as rmps;

use self::pending::*;
use crate::util::{buddy::*, likely::*};
use crate::{msgbuf::*, nexus::*, pkthdr::*, request::*, session::*, transport::*, type_alias::*};

/// Interior-mutable state of an [`Rpc`] instance.
pub(crate) struct RpcInterior {
    /// Sessions.
    sessions: Vec<Session>,

    /// Buffer allocator.
    allocator: *mut BuddyAllocator,

    /// RDMA UD transport layer.
    tp: UdTransport,

    /// Pending RPC handlers.
    pending_handlers: Vec<PendingHandler>,
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

    /// Session management packet sender.
    /// Use a independent socket, no need to delegate to the Nexus.
    sm_tx: UdpSocket,
    /// Session management event receiver.
    sm_rx: SmEventRx,

    /// Interior-mutable state of this RPC.
    state: RefCell<RpcInterior>,

    /// Pending packet transmissions.
    ///
    /// Placed in a separate `RefCell`, so that when switched to RPC handler contexts,
    /// we do not need to borrow `RpcInterior` again.
    pending_tx: RefCell<Vec<TxItem>>,
}

// Internal progress.Rx routines.
impl Rpc {
    /// Process a single-packet incoming request.
    /// Return `true` if the RPC handler returns immediately or not even called
    /// (which means the caller can release the Rx buffer).
    #[must_use = "you must the return value to decide whether to release the Rx buffer"]
    fn process_small_request(
        &self,
        state: &mut RpcInterior,
        sess_id: SessId,
        sslot_idx: usize,
        hdr: &PacketHeader,
    ) -> bool {
        debug_assert_eq!(
            hdr.pkt_type(),
            PktType::SmallReq,
            "packet type is not small-request"
        );

        // Check request type sanity.
        let req_type = hdr.req_type();
        if unlikely(!self.nexus.has_rpc_handler(req_type)) {
            log::warn!(
                "RPC {}: dropping received SmallRequest for unknown request type {:?}",
                self.id,
                req_type
            );
            return true;
        }

        let sess = &mut state.sessions[sess_id as usize];
        let sslot = &mut sess.slots[sslot_idx];

        // Check request index sanity.
        let req_idx = hdr.req_idx();
        if unlikely(req_idx <= sslot.req_idx) {
            // Simply drop outdated requests.
            if req_idx < sslot.req_idx {
                log::warn!(
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
                    log::info!(
                        "RPC {}: retransmitting possibly-lost response for request (idx {}, ty {})",
                        self.id,
                        req_idx,
                        req_type
                    );
                    self.pending_tx.borrow_mut().push(TxItem {
                        peer: sess.peer.as_ref().unwrap(),
                        msgbuf: &sslot.resp,
                    });
                } else {
                    // If the request is simply not finished, we have nothing to do.
                    // Let the client wait.
                    log::info!(
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

        // SAFETY: `hdr` is not null and properly aligned, and is right before the application data.
        sslot.req = unsafe {
            MsgBuf::borrowed(
                NonNull::new_unchecked(hdr as *const _ as *mut _),
                hdr.data_len() as _,
                0,
            )
        };

        // Call the request handler to get a unexecuted future.
        let req = Request::new(sslot);
        let mut resp_fut = self.nexus.call_rpc_handler(req);

        // Immediately poll the future.
        // This will make synchronous handlers return, and push asynchronous handlers
        // into the first yield point.
        let mut cx = Context::from_waker(noop_waker_ref());
        match resp_fut.poll_unpin(&mut cx) {
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

                // Push the packet to the pending TX queue.
                self.pending_tx.borrow_mut().push(TxItem {
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
            log::warn!(
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
    fn process_sm_events(&self) {
        // Abort if progressing recursively.
        let Ok(mut state) = self.state.try_borrow_mut() else {
            return;
        };
        let state: &mut RpcInterior = &mut state;

        while let Some(event) = self.sm_rx.recv() {
            log::trace!("RPC {}: received SM event {:#?}", self.id, event);
            debug_assert_eq!(event.dst_rpc_id, self.id, "bad SM event dispatch");

            match event.details {
                SmEventDetails::ConnectRequest {
                    uri,
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
                    let svr_ep = rmps::to_vec(&state.tp.endpoint())
                        .expect("failed to serialize local endpoint");

                    let mut sess = Session::new(self, SessionRole::Server);
                    sess.peer_rpc_id = event.src_rpc_id;
                    sess.peer_sess_id = cli_sess_id;
                    sess.peer = Some(peer);

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
                    let ack_buf =
                        rmps::to_vec(&ack).expect("failed to serialize ConnectAcknowledge");
                    self.sm_tx
                        .send_to(&ack_buf, uri)
                        .expect("failed to send ConnectAcknowledge");
                }
                SmEventDetails::ConnectAcknowledge {
                    cli_sess_id,
                    svr_ep,
                    svr_sess_id,
                } => {
                    if state.sessions.len() <= cli_sess_id as usize {
                        log::warn!(
                            "RPC {}: ignoring ConnectAcknowledge for non-existent session {}",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    }

                    let sess = &mut state.sessions[cli_sess_id as usize];
                    if sess.is_connected() {
                        log::warn!(
                            "RPC {}: ignoring ConnectAcknowledge for already-connected session {}",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    }
                    if sess.is_client() {
                        log::warn!(
                            "RPC {}: ignoring ConnectAcknowledge for client session {}",
                            self.id,
                            cli_sess_id
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
                }
                SmEventDetails::ConnectRefuse {
                    cli_sess_id,
                    reason,
                } => {
                    if state.sessions.len() <= cli_sess_id as usize {
                        log::warn!(
                            "RPC {}: ignoring ConnectRefuse for non-existent session {}",
                            self.id,
                            cli_sess_id
                        );
                        continue;
                    }

                    log::warn!(
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
        // Abort if progressing recursively.
        let Ok(mut state) = self.state.try_borrow_mut() else {
            return;
        };
        let state: &mut RpcInterior = &mut state;

        // Do RX burst.
        let n = state.tp.rx_burst();

        // Process the received packets.
        let mut rx_bufs_to_release = Vec::with_capacity(n);
        for _ in 0..n {
            let item = state.tp.rx_next().expect("failed to fetch received packet");

            // SAFETY: guaranteed not null and aligned.
            let hdr = unsafe { NonNull::new_unchecked(item.pkt_hdr()).as_mut() };
            if unlikely(state.sessions.len() as u32 <= hdr.dst_sess_id()) {
                log::warn!(
                    "RPC {}: dropping received data-plane packet for non-existent session {}",
                    self.id,
                    hdr.dst_sess_id()
                );
            }

            // Perform session sanity check.
            let sess = &state.sessions[hdr.dst_sess_id() as usize];
            if unlikely(!sess.is_connected()) {
                log::warn!(
                    "RPC {}: dropping received data-plane packet for non-connected session {}",
                    self.id,
                    hdr.dst_sess_id()
                );
            }
            match hdr.pkt_type() {
                PktType::SmallReq | PktType::LargeReqCtrl => {
                    if unlikely(sess.is_client()) {
                        log::warn!(
                            "RPC {}: dropping received {:?} for client session {}",
                            self.id,
                            hdr.pkt_type(),
                            hdr.dst_sess_id()
                        );
                        continue;
                    }
                }
                PktType::SmallResp | PktType::LargeRespCtrl => {
                    if unlikely(!sess.is_client()) {
                        log::warn!(
                            "RPC {}: dropping received {:?} for server session {}",
                            self.id,
                            hdr.pkt_type(),
                            hdr.dst_sess_id()
                        );
                        continue;
                    }
                }
            }

            // Perform packet sanity check.
            if matches!(hdr.pkt_type(), PktType::SmallReq | PktType::SmallResp) {
                if unlikely(hdr.data_len() > UdTransport::max_data_in_pkt() as u32) {
                    log::warn!(
                        "RPC {}: dropping received {:?} with too large data length {}",
                        self.id,
                        hdr.pkt_type(),
                        hdr.data_len()
                    );
                    continue;
                }
            }

            // Trigger packet processing logic.
            let sslot_idx = hdr.req_idx() as usize % ACTIVE_REQ_WINDOW;
            match hdr.pkt_type() {
                PktType::SmallReq => {
                    if self.process_small_request(state, hdr.dst_sess_id(), sslot_idx, hdr) {
                        rx_bufs_to_release.push(item);
                    }
                }
                PktType::LargeReqCtrl => todo!("long request"),
                PktType::SmallResp => {
                    self.process_small_response(
                        state,
                        hdr.dst_sess_id(),
                        sslot_idx,
                        hdr,
                        item.as_ptr(),
                    );
                    rx_bufs_to_release.push(item);
                }
                PktType::LargeRespCtrl => todo!("long response"),
            }
        }

        // Release the Rx buffers to the transport layer.
        // SAFETY: `rx_bufs_to_release` contains valid pointers to Rx buffers, and each buffer
        // is only released once (which is this release).
        if !rx_bufs_to_release.is_empty() {
            unsafe { state.tp.rx_release(&rx_bufs_to_release) };
        }
    }

    /// Transmit pending packets.
    fn process_tx(&self) {
        // Abort if progressing recursively.
        let Ok(mut state) = self.state.try_borrow_mut() else {
            return;
        };
        let state: &mut RpcInterior = &mut state;

        let mut pending_tx = self.pending_tx.borrow_mut();
        if unlikely(!pending_tx.is_empty()) {
            // SAFETY: items in `pending_tx` all points to valid peers and `MsgBuf`s,
            // which is guaranteed by `process_rx()` and `process_pending_handlers()`.
            unsafe { state.tp.tx_burst(&pending_tx) };
            pending_tx.clear();
        }
    }

    /// Poll pending RPC handlers.
    fn process_pending_handlers(&self) {
        todo!()
    }
}

// Public API.
impl Rpc {
    /// Create a new `Rpc` instance that is bound to a [`Nexus`] with a certain
    /// ID. Will operate on the specified port of the given device.
    /// The given ID must be unique among all RPCs in the same Nexus.
    ///
    /// # Panics
    ///
    /// - Panic if the given ID is already used.
    /// - Panic if there is no such device or no such port.
    pub fn new(nexus: &Pin<Arc<Nexus>>, id: RpcId, nic: &str, phy_port: u8) -> Self {
        // Create the SM event channel first, so that it will immediately
        // panic if the given ID is already used.
        let sm_rx = nexus.register_event_channel(id);
        Self {
            id,
            nexus: nexus.clone(),
            sm_tx: UdpSocket::bind("0.0.0.0:0").unwrap(),
            sm_rx,
            state: RefCell::new(RpcInterior {
                sessions: Vec::new(),
                allocator: Box::into_raw(Box::new(BuddyAllocator::new())),
                tp: UdTransport::new(nic, phy_port),
                pending_handlers: Vec::new(),
            }),
            pending_tx: RefCell::new(Vec::new()),
        }
    }

    /// Return the ID of this RPC instance.
    #[inline(always)]
    pub fn id(&self) -> RpcId {
        self.id
    }

    /// Allocate a `MsgBuf` that can accommodate at least `len` bytes of
    /// application data.
    ///
    /// The allocated `MsgBuf` will have an initial length of `len`, but the
    /// contents are uninitialized.
    #[inline]
    pub fn alloc_msgbuf(&self, len: usize) -> MsgBuf {
        let mut state = self.state.borrow_mut();

        // SAFETY: validity of `allocator` ensured by constructor.
        let allocator = Pin::new(unsafe { &mut *state.allocator });
        let buf = allocator.alloc(len + mem::size_of::<PacketHeader>(), &mut state.tp);
        MsgBuf::owned(buf, len)
    }

    /// Run an iteration of event loop to make progress.
    /// Performs tasks including:
    /// - handling session connection requests and responses,
    /// - hanlding datapath requests and responses,
    /// - scheduling and (re)transmitting datapath packets.
    #[inline]
    pub fn progress(&self) {
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
        // Cleanup raw pointers.
        {
            let state = self.state.borrow_mut();

            // SAFETY: the allocator is built by `Box::from_raw`, now just reclaim it.
            unsafe {
                drop(Box::from_raw(state.allocator));
            }
        }

        // Destroy the SM event channel.
        self.nexus.destroy_event_channel(self.id);
    }
}
