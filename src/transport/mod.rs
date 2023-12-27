mod infiniband;

use std::iter::{self, Iterator};
use std::mem;

use serde::{de::DeserializeOwned, Serialize};

use crate::msgbuf::MsgBuf;
use crate::pkthdr::PacketHeader;

/// Memory region handle type.
pub type LocalKey = u32;

/// An item to transmit.
pub(crate) struct TxItem<Tp: UnreliableTransport> {
    /// Peer for this packet.
    pub peer: *mut Tp::Peer,

    /// Message buffer.
    pub msgbuf: *mut MsgBuf,

    /// Index of this packet in the entire message.
    pub pkt_idx: usize,
}

/// Trait for generalized possibly-unreliable transport.
///
/// This trait is not fault-tolerant. Return types of all methods are success
/// types instead of `Result`s. If an error occurs, the thread is expected to
/// panic.
pub(crate) trait UnreliableTransport: Sized {
    /// Endpoint information type, used for in-network data exchange.
    /// Must not contain any data dependent on local resources.
    type Endpoint: Sized + Clone + Copy + Serialize + DeserializeOwned + 'static;

    /// Peer information type, used for sending data.
    /// Can contain data dependent on local resources, e.g., `*mut ibv_ah`.
    type Peer: Sized;

    /// Create a new transport instance that is bound to a specific port on a
    /// specific NIC.
    fn new(nic: &str, phy_port: u8) -> Self;

    /// Return the MTU of the transport.
    fn mtu() -> usize;

    /// Return the maximum amount of data in a packet.
    /// This is computed as subtracting `mem::size_of<PacketHeader>()` from MTU.
    #[inline(always)]
    fn max_data_per_pkt() -> usize {
        Self::mtu() - mem::size_of::<PacketHeader>()
    }

    /// Return the endpoint information representing the transport instance.
    fn endpoint(&self) -> Self::Endpoint;

    /// Construct a peer from the given endpoint information.
    fn make_peer(&self, ep: Self::Endpoint) -> Self::Peer;

    /// Register memory so that it is accessible by the transport.
    /// Return a handle to the registered memory region.
    ///
    /// # Safety
    ///
    /// The memory region `[buf, buf + len)` must be valid for access.
    unsafe fn reg_mem(&mut self, buf: *mut u8, len: usize) -> LocalKey;

    /// Transmit a batch of messages.
    ///
    /// # Safety
    ///
    /// The items in the batch must all be valid.
    unsafe fn tx_burst(&mut self, items: &[TxItem<Self>]);

    /// Drain transmission queue.
    fn tx_drain(&mut self);

    /// Receive a batch of messages.
    /// Return the number of messages received.
    fn rx_burst(&mut self) -> usize;

    /// Return the next received message.
    fn rx_next(&mut self) -> Option<MsgBuf>;

    /// Mark received messages as released and can be reused.
    ///
    /// # Safety
    ///
    /// - Only `MsgBuf` returned by `rx_next` can be released.
    /// - Every `MsgBuf` must not be used after it is released.
    /// - Every `MsgBuf` must not be released more than once.
    unsafe fn rx_release(&mut self, items: impl Iterator<Item = MsgBuf>);

    /// Mark a received message as released and can be reused.
    /// This is a convenience method for `rx_release` on [`iter::once(item)`].
    ///
    /// # Safety
    ///
    /// - Only `MsgBuf` returned by `rx_next` can be released.
    /// - Every `MsgBuf` must not be used after it is released.
    /// - Every `MsgBuf` must not be released more than once.
    unsafe fn rx_release_one(&mut self, item: MsgBuf) {
        self.rx_release(iter::once(item))
    }
}

pub use infiniband::*;
