#![allow(private_bounds)]

mod pending;

use std::cell::RefCell;
use std::net::UdpSocket;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;

use rmp_serde as rmps;

use self::pending::*;
use crate::nexus::{Nexus, SmEvent, SmEventDetails, SmEventRx};
use crate::pkthdr::*;
use crate::session::*;
use crate::transport::*;
use crate::type_alias::*;
use crate::util::{buddy::*, likely::*};

/// Interior-mutable state of an [`Rpc`] instance.
pub(crate) struct RpcInterior<Tp: UnreliableTransport> {
    /// Sessions.
    sessions: Vec<Session<Tp>>,

    /// Buffer allocator.
    buddy: BuddyAllocator,

    /// Transport layer.
    tp: Tp,

    /// Pending RPC handlers.
    pending_handlers: RefCell<Vec<PendingHandler>>,
}

/// Thread-local RPC endpoint.
///
/// This type accepts a generic type that specifies the transport layer.
/// Available transports are provided in the [`crate::transport`] module.
///
/// This is the main type of this library.
pub struct Rpc<Tp: UnreliableTransport> {
    /// ID of this RPC instance.
    id: RpcId,
    /// Nexus this RPC is bound to.
    nexus: Pin<Arc<Nexus>>,

    /// Session management packet sender.
    sm_tx: UdpSocket,
    /// Session management event receiver.
    sm_rx: SmEventRx,

    /// Interior-mutable state of this RPC.
    state: RefCell<RpcInterior<Tp>>,

    /// Pending packet transmissions.
    ///
    /// Placed in a separate `RefCell`, so that when switched to RPC handler contexts,
    /// we do not need to borrow `RpcInterior` again.
    pending_tx: RefCell<Vec<TxItem<Tp>>>,
}

/// Internal progress.Rx routines.
impl<Tp: UnreliableTransport> Rpc<Tp> {
    fn process_small_request(&self, sslot: &mut SSlot, hdr: &mut PacketHeader) {
        todo!()
    }

    fn process_response(&self, sslot: &mut SSlot, hdr: &mut PacketHeader) {
        todo!()
    }
}

/// Internal progress routines.
impl<Tp: UnreliableTransport> Rpc<Tp> {
    /// Process received session management events.
    fn process_sm_events(&self) {
        // Abort if progressing recursively.
        let Ok(mut state) = self.state.try_borrow_mut() else {
            return;
        };
        let state: &mut RpcInterior<Tp> = &mut state;

        while let Some(event) = self.sm_rx.recv() {
            log::trace!("RPC {}: received SM event {:#?}", self.id, event);
            debug_assert_eq!(event.dst_rpc_id, self.id, "bad SM event dispatch");

            match event.details {
                SmEventDetails::ConnectRequest {
                    uri,
                    cli_ep,
                    cli_sess_id,
                    credits,
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
                    let peer = state.tp.make_peer(cli_ep);
                    let svr_ep = rmps::to_vec(&state.tp.endpoint())
                        .expect("failed to serialize local endpoint");

                    let mut sess = Session::new(self, SessionRole::Server, credits);
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
                    let peer = state.tp.make_peer(svr_ep);

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
        let state: &mut RpcInterior<Tp> = &mut state;

        // Do RX burst.
        let n = state.tp.rx_burst();

        // Fetch the received packets.
        for _ in 0..n {
            let item = state.tp.rx_next().expect("failed to fetch received packet");

            // SAFETY: guaranteed not null and aligned.
            let hdr = unsafe { NonNull::new_unchecked(item.pkt_hdr(0)).as_mut() };
            if unlikely(state.sessions.len() as u32 <= hdr.dst_sess_id()) {
                log::warn!(
                    "RPC {}: dropping received data-plane packet for non-existent session {}",
                    self.id,
                    hdr.dst_sess_id()
                );
            }

            let sess = &mut state.sessions[hdr.dst_sess_id() as usize];
            if unlikely(!sess.is_connected()) {
                log::warn!(
                    "RPC {}: dropping received data-plane packet for non-connected session {}",
                    self.id,
                    hdr.dst_sess_id()
                );
            }

            let sslot_idx = hdr.req_idx() as usize % ACTIVE_REQ_WINDOW;
            let sslot = &mut sess.slots[sslot_idx];
            match hdr.pkt_type() {
                PktType::Req => {
                    if likely(hdr.data_len() <= Tp::max_data_per_pkt() as u32) {
                        self.process_small_request(sslot, hdr);
                    } else {
                        todo!("multi-packet message")
                    }
                }
                PktType::Resp => self.process_response(sslot, hdr),
                PktType::Rfr => todo!(),
                PktType::ExplCR => todo!(),
            }
        }
    }

    /// Transmit pending packets.
    fn process_tx(&self) {
        todo!()
    }

    /// Poll pending RPC handlers.
    fn poll_pending_handlers(&self) {
        todo!()
    }
}

/// Thread-local RPC endpoint.
impl<Tp: UnreliableTransport> Rpc<Tp> {
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
                buddy: BuddyAllocator::new(),
                tp: Tp::new(nic, phy_port),
                pending_handlers: RefCell::new(Vec::new()),
            }),
            pending_tx: RefCell::new(Vec::new()),
        }
    }

    /// Return the ID of this RPC instance.
    #[inline(always)]
    pub fn id(&self) -> RpcId {
        self.id
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
        self.poll_pending_handlers();
        self.process_rx();
        self.process_tx();
    }
}

impl<Tp: UnreliableTransport> Drop for Rpc<Tp> {
    fn drop(&mut self) {
        self.nexus.destroy_event_channel(self.id);
    }
}
