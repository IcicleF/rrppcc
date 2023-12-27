mod sslot;

use std::array;
use std::collections::VecDeque;

pub(crate) use self::sslot::*;
use crate::msgbuf::MsgBuf;
use crate::rpc::Rpc;
use crate::transport::UnreliableTransport;
use crate::type_alias::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionRole {
    Client,
    Server,
}

struct PendingRequest {
    /// Request type.
    req_type: ReqType,

    /// Request MsgBuf.
    req_msgbuf: *mut MsgBuf,

    /// Response MsgBuf.
    resp_msgbuf: *mut MsgBuf,
}

pub(crate) const ACTIVE_REQ_WINDOW: usize = 8;

pub(crate) struct Session<Tp: UnreliableTransport> {
    /// Role of this session.
    role: SessionRole,

    /// Remote peer's Rpc ID.
    pub peer_rpc_id: RpcId,

    /// Remote peer's session ID.
    pub peer_sess_id: SessId,

    /// Remote peer routing information.
    pub peer: Option<Tp::Peer>,

    /// Session credits.
    credits: usize,

    /// Session request slots.
    pub slots: [SSlot; ACTIVE_REQ_WINDOW],

    /// Queue for requests that are waiting for credits.
    req_backlog: VecDeque<PendingRequest>,
}

impl<Tp: UnreliableTransport> Session<Tp> {
    /// Create a new session.
    pub fn new(rpc: &Rpc<Tp>, role: SessionRole, credits: usize) -> Self {
        // FIXME: initialize slots.
        let slots = array::from_fn(|_| todo!());
        Self {
            role,
            peer_rpc_id: rpc.id(),
            peer_sess_id: 0,
            peer: None,
            credits,
            slots,
            req_backlog: VecDeque::new(),
        }
    }

    /// Return `true` if this session is a client, otherwise server.
    #[inline(always)]
    pub fn is_client(&self) -> bool {
        self.role == SessionRole::Client
    }

    /// Return `true` if this session is connected.
    #[inline(always)]
    pub fn is_connected(&self) -> bool {
        self.peer.is_some()
    }
}
