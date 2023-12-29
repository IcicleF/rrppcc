use std::collections::VecDeque;
use std::mem;
use std::ptr::{self, NonNull};

use rrddmma::{
    bindings::*,
    prelude::*,
    rdma::{
        mr::Permission,
        qp::{QpEndpoint, QpPeer},
    },
};

use crate::msgbuf::MsgBuf;
use crate::pkthdr::PacketHeader;
use crate::util::{huge_alloc::*, likely::*};

/// Memory region handle type.
pub type LKey = rrddmma::rdma::type_alias::LKey;

/// An item to transmit.
pub(crate) struct TxItem {
    /// Peer for this packet.
    pub peer: *const QpPeer,

    /// Message buffer.
    pub msgbuf: *const MsgBuf,
}

/// Received but unreturned message metadata.
struct RxItem {
    /// Receive unit index.
    idx: u16,

    /// Message length in bytes.
    len: u16,
}

/// RDMA UD transport.
pub(crate) struct UdTransport {
    /// The UD queue pair.
    qp: Qp,
    /// Memory region registry.
    mrs: Vec<Mr>,

    /// Send packet sequence number.
    /// Used to check whether to signal a batch of send requests.
    tx_pkt_idx: usize,
    /// Send SGE buffer.
    tx_sgl: Vec<ibv_sge>,
    /// Send work request buffer.
    tx_wr: Vec<ibv_send_wr>,

    /// Recv memory buffer.
    // Place after `mrs` to ensure that it is dropped after memory regions.
    #[allow(unused)]
    rx_buf: HugeAlloc,
    /// Recv SGE buffer.
    #[allow(unused)]
    rx_sge: Vec<ibv_sge>,
    /// Recv work request buffer.
    rx_wr: Vec<ibv_recv_wr>,
    /// Recv work completion buffer.
    rx_wc: Vec<Wc>,
    /// Received but unreturned messages.
    rx_items: VecDeque<RxItem>,
    /// Number of pending receive work requests to repost.
    rx_repost_pending: usize,

    /// Myself as a peer. Used for `tx_flush`.
    peer_to_myself: QpPeer,
}

const CACHELINE_SIZE: usize = 64;

impl UdTransport {
    const ROUTING_INFO_LEN: usize = 32;

    const GRH_SIZE: usize = 40;
    const MTU: usize = 1 << 12;
    const MAX_PKT_SIZE: usize = Self::MTU - mem::size_of::<PacketHeader>();

    const SQ_SIZE: usize = 1 << 8;
    const SQ_SIGNAL_BATCH: usize = 1 << 6;

    const RQ_SIZE: usize = 1 << 12;
    const RQ_POLL_BATCH: usize = 1 << 4;
    const RQ_POSTLIST_SIZE: usize = 1 << 6;
    const RX_UNIT_ALLOC_SIZE: usize = CACHELINE_SIZE + Self::MTU;
    const RX_UNIT_SIZE: usize = Self::GRH_SIZE + Self::MTU;
}

impl UdTransport {
    /// Get the offset of the `i`-th receive unit in the entire buffer.
    /// This will point to the beginning of the GRH.
    #[inline(always)]
    const fn rx_offset(i: usize) -> usize {
        i * UdTransport::RX_UNIT_ALLOC_SIZE + (CACHELINE_SIZE - UdTransport::GRH_SIZE)
    }

    /// Get the offset of the `i`-th receive unit's payload in the entire buffer.
    /// This will point to the beginning of the payload, usually the packet header.
    #[inline(always)]
    const fn rx_payload_offset(i: usize) -> usize {
        i * UdTransport::RX_UNIT_ALLOC_SIZE + CACHELINE_SIZE
    }
}

impl UdTransport {
    /// Create a new transport instance that is bound to a specific port on a
    /// specific NIC.
    pub fn new(nic: &str, phy_port: u8) -> Self {
        assert!(
            CACHELINE_SIZE >= Self::GRH_SIZE,
            "GRH too large, cannot fit in cacheline"
        );
        assert!(
            Self::RQ_SIZE.trailing_zeros() < LKey::BITS,
            "too many recv units, index cannot fit in LocalKey"
        );

        // Initialize QP.
        let Nic { context, ports } = Nic::finder()
            .dev_name(nic)
            .port_num(phy_port)
            .probe_nth_port(0)
            .expect("failed to find target NIC or physical port");
        let port = ports.into_iter().next().unwrap();
        assert!(
            port.mtu().bytes() == Self::mtu(),
            "path active MTU must be 4KiB"
        );

        let pd = Pd::new(&context).expect("failed to allocate protection domain");
        let qp = {
            let send_cq =
                Cq::new(&context, Self::SQ_SIZE as _).expect("failed to allocate UD send CQ");
            let recv_cq =
                Cq::new(&context, Self::RQ_SIZE as _).expect("failed to allocate RC recv CQ");
            let mut qp = Qp::builder()
                .qp_type(QpType::Ud)
                .send_cq(&send_cq)
                .recv_cq(&recv_cq)
                .caps(QpCaps {
                    max_send_wr: Self::SQ_SIZE as _,
                    max_recv_wr: Self::RQ_SIZE as _,
                    max_send_sge: 1,
                    max_recv_sge: 1,
                    ..QpCaps::default()
                })
                .sq_sig_all(false)
                .build(&pd)
                .expect("failed to create UD queue pair");
            qp.bind_local_port(&port, None)
                .expect("failed to bind UD QP to port");
            qp
        };

        // Create a peer for myself.
        let peer_to_myself = qp
            .make_peer(&qp.endpoint().unwrap())
            .expect("failed to create peer for myself");

        // Initialize send WRs.
        let mut tx_sgl = vec![ibv_sge::default(); Self::SQ_SIZE + 1];
        let mut tx_wr = (0..(Self::SQ_SIZE + 1))
            .map(|i| ibv_send_wr {
                wr_id: i as _,
                sg_list: &mut tx_sgl[i],
                num_sge: 1,
                opcode: ibv_wr_opcode::IBV_WR_SEND,
                ..unsafe { mem::zeroed() }
            })
            .collect::<Vec<_>>();
        for i in 0..Self::SQ_SIZE {
            tx_wr[i].next = &mut tx_wr[i + 1] as *mut _;
        }

        // Initialize recv buffer.
        let rx_buf = alloc_raw(Self::RQ_SIZE * Self::RX_UNIT_ALLOC_SIZE);
        assert!(rx_buf.ptr as usize % CACHELINE_SIZE == 0);

        // SAFETY: correct allocated buffer.
        let rx_mr = unsafe {
            Mr::reg(&pd, rx_buf.ptr, rx_buf.len, Permission::LOCAL_WRITE)
                .expect("failed to register recv memory region")
        };

        // Initialize recv WRs.
        let mut rx_sge = (0..Self::RQ_POSTLIST_SIZE)
            .map(|_| ibv_sge {
                addr: 0,
                length: Self::RX_UNIT_SIZE as _,
                lkey: rx_mr.lkey(),
            })
            .collect::<Vec<_>>();

        let mut rx_wr = (0..Self::RQ_POSTLIST_SIZE)
            .map(|i| ibv_recv_wr {
                sg_list: &mut rx_sge[i],
                num_sge: 1,
                ..unsafe { mem::zeroed() }
            })
            .collect::<Vec<_>>();
        for i in 0..(Self::RQ_POSTLIST_SIZE - 1) {
            rx_wr[i].next = &mut rx_wr[i + 1];
        }

        // Post recv WRs in batches.
        for start in (0..Self::RQ_SIZE).step_by(Self::RQ_POSTLIST_SIZE) {
            let end = (start + Self::RQ_POSTLIST_SIZE).min(Self::RQ_SIZE);

            for i in 0..(end - start) {
                let offset = Self::rx_offset(start + i);

                // SAFETY: in the same allocated buffer.
                rx_sge[i].addr = unsafe { rx_buf.ptr.add(offset) } as _;
                rx_wr[i].wr_id = (start + i) as _;
            }

            // SAFETY: FFI.
            unsafe {
                qp.post_raw_recv(&rx_wr[0])
                    .expect("failed to post recv WRs");
            }
        }

        // Initialize recv WC buffer.
        let rx_wc = vec![Wc::default(); Self::RQ_POLL_BATCH];
        let rx_items = VecDeque::with_capacity(Self::RQ_POLL_BATCH);

        Self {
            qp,
            mrs: vec![rx_mr],

            tx_pkt_idx: 0,
            tx_sgl,
            tx_wr,

            rx_buf,
            rx_sge,
            rx_wr,
            rx_wc,
            rx_items,
            rx_repost_pending: 0,

            peer_to_myself,
        }
    }

    /// Return the MTU of the transport.
    /// The MTU is the maximum data amount of a single packet.
    #[inline(always)]
    pub const fn mtu() -> usize {
        Self::MTU
    }

    /// Return the maximum amount of data that can be sent in a single packet.
    /// This is the MTU minus the size of the packet header.
    #[inline(always)]
    pub const fn max_data_in_pkt() -> usize {
        Self::MAX_PKT_SIZE
    }

    /// Return serialized endpoint information representing the transport instance.
    pub fn endpoint(&self) -> QpEndpoint {
        self.qp.endpoint().unwrap()
    }

    /// Construct a peer from the given endpoint information.
    pub fn create_peer(&self, ep: QpEndpoint) -> QpPeer {
        self.qp
            .make_peer(&ep)
            .unwrap_or_else(|_| panic!("failed to create peer from endpoint {:?}", ep))
    }

    /// Register memory so that it is accessible by the transport.
    /// Return a handle to the registered memory region.
    ///
    /// # Safety
    ///
    /// The memory region `[buf, buf + len)` must be valid for access.
    pub unsafe fn reg_mem(&mut self, buf: *mut u8, len: usize) -> LKey {
        let mr = Mr::reg(self.qp.pd(), buf, len, Permission::default())
            .expect("failed to register memory region");
        let lkey = mr.lkey();
        self.mrs.push(mr);
        lkey
    }

    /// Transmit a batch of messages.
    ///
    /// # Safety
    ///
    /// The items in the batch must all be valid.
    pub unsafe fn tx_burst(&mut self, items: &[TxItem]) {
        if items.is_empty() {
            return;
        }

        // Split the many packets into batches.
        // Batch size must not be larger than `SQ_SIGNAL_BATCH`, or it will stuck
        // when trying to poll CQEs from unposted WRs and break the invariant that
        // there is always exactly one unpolled CQE (after the first Tx packet).
        if unlikely(items.len() > Self::SQ_SIGNAL_BATCH) {
            // SAFETY: recursion (induction).
            for chunk in items.chunks(Self::SQ_SIGNAL_BATCH) {
                self.tx_burst(chunk);
            }
            return;
        }

        for (i, item) in items.iter().enumerate() {
            // SAFETY: the caller ensures that memory handles are valid.
            let sge = &mut self.tx_sgl[i];
            let wr = &mut self.tx_wr[i];
            debug_assert_eq!(wr.sg_list, sge as *mut _);

            // Set signaled flag + poll send CQ if needed.
            wr.send_flags = if self.tx_pkt_idx % Self::SQ_SIGNAL_BATCH == 0 {
                if self.tx_pkt_idx > 0 {
                    self.qp.scq().poll_one_blocking_consumed();
                }
                ibv_send_flags::IBV_SEND_SIGNALED.0
            } else {
                0
            };
            self.tx_pkt_idx += 1;

            // Fill in the scatter/gather list.
            let msgbuf = &*item.msgbuf;
            let length = msgbuf.len() as _;
            *sge = ibv_sge {
                addr: msgbuf.pkt_hdr() as _,
                length,
                lkey: msgbuf.lkey(),
            };
            if length <= self.qp.caps().max_inline_data {
                wr.send_flags |= ibv_send_flags::IBV_SEND_INLINE.0;
            }

            // Fill in routing information.
            // Safety requirements should be upheld by the caller, no need to check here.
            wr.wr.ud = unsafe { (*item.peer).ud() };
        }

        // Break the linked list chain.
        self.tx_wr[items.len() - 1].next = ptr::null_mut();

        // SAFETY: all work requests are correctly constructed.
        self.qp
            .post_raw_send(&self.tx_wr[0])
            .expect("failed to post send WRs");

        // Restore the linked list chain.
        self.tx_wr[items.len() - 1].next = &mut self.tx_wr[items.len()] as *mut _;
    }

    /// Drain transmission queue.
    pub fn tx_drain(&mut self) {
        if unlikely(self.tx_pkt_idx == 0) {
            return;
        }

        // There must be exactly one unpolled CQE. Poll it.
        self.qp.scq().poll_one_blocking_consumed();

        // Send a packet that will be dropped by myself.
        let buf = [0u8; 1];
        let mut sge = ibv_sge {
            addr: buf.as_ptr() as _,
            length: 1,
            lkey: 0,
        };

        let mut wr = ibv_send_wr {
            sg_list: &mut sge,
            num_sge: 1,
            opcode: ibv_wr_opcode::IBV_WR_SEND,
            send_flags: (ibv_send_flags::IBV_SEND_INLINE | ibv_send_flags::IBV_SEND_SIGNALED).0,

            // SAFETY: POD type.
            ..unsafe { mem::zeroed() }
        };

        self.peer_to_myself.set_ud_peer(&mut wr);
        wr.wr.ud.remote_qpn = 0;

        // SAFETY: all work requests are correctly constructed.
        unsafe {
            self.qp
                .post_raw_send(&wr)
                .expect("failed to post send WR (tx_flush)");
        }

        // Poll the CQE.
        self.qp.scq().poll_one_blocking_consumed();

        // Reset signal counter.
        self.tx_pkt_idx = 0;
    }

    /// Receive a batch of messages.
    /// Return the number of messages received.
    pub fn rx_burst(&mut self) -> usize {
        let n = self
            .qp
            .rcq()
            .poll_into(&mut self.rx_wc)
            .expect("failed to poll recv CQ") as usize;
        for i in 0..n {
            let wc = &self.rx_wc[i];
            self.rx_items.push_back(RxItem {
                idx: wc.wr_id() as _,
                len: wc.ok().expect("failed to recv") as _,
            });
        }
        n
    }

    /// Return the next received message.
    #[inline]
    pub fn rx_next(&mut self) -> Option<MsgBuf> {
        let RxItem { idx, len } = self.rx_items.pop_front()?;
        let offset = Self::rx_payload_offset(idx as _);

        // SAFETY: pointer guaranteed not-null, and within the same allocated buffer.
        let buf = unsafe { NonNull::new_unchecked(self.rx_buf.ptr.add(offset) as *mut _) };

        // Embed the index into the unused `lkey` so that we do not need to perform division
        // to recover it from the pointer during release.
        // SAFETY: the recv buffer layout ensures the buffer's validity.
        let msgbuf = unsafe { MsgBuf::borrowed(buf, len as _, idx as _) };
        Some(msgbuf)
    }

    /// Mark received messages as released and can be reused.
    ///
    /// # Safety
    ///
    /// - Only `MsgBuf` returned by `rx_next` can be released.
    /// - Every `MsgBuf` must not be used after it is released.
    /// - Every `MsgBuf` must not be released more than once.
    pub unsafe fn rx_release(&mut self, items: &[MsgBuf]) {
        // Record the released buffer.
        for item in items {
            self.rx_sge[self.rx_repost_pending].addr = item.pkt_hdr() as _;
            self.rx_wr[self.rx_repost_pending].wr_id = item.lkey() as _;
            self.rx_repost_pending += 1;

            if unlikely(self.rx_repost_pending == Self::RQ_POSTLIST_SIZE) {
                // SAFETY: all work requests are correctly constructed.
                self.qp
                    .post_raw_recv(&self.rx_wr[0])
                    .expect("failed to post recv WRs");
                self.rx_repost_pending = 0;
            }
        }
    }

    /// Mark a received message as released and can be reused.
    /// This is a convenience method for `rx_release` on [`iter::once(item)`].
    ///
    /// # Safety
    ///
    /// - Only `MsgBuf` returned by `rx_next` can be released.
    /// - Every `MsgBuf` must not be used after it is released.
    /// - Every `MsgBuf` must not be released more than once.
    #[inline(always)]
    pub unsafe fn rx_release_one(&mut self, item: MsgBuf) {
        self.rx_release(&[item])
    }
}
