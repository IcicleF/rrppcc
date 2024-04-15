use crate::msgbuf::MsgBuf;
use crate::rpc::RpcInterior;
use crate::session::SessionRole;
use crate::transport::UdTransport;
use crate::type_alias::*;

pub(crate) struct SSlot {
    /// Current request index.
    pub req_idx: ReqIdx,

    /// Current request type.
    /// Only used by server, so for client, this is always zero.
    pub req_type: ReqType,

    /// Request MsgBuf.
    /// - For server: this is the request from client.
    ///   It can be either a borrowed buffer from the transport (for zero-copy for small requests),
    ///   or a dynamically allocated buffer (for large requests).
    /// - For client: this is the user-provided request buffer.
    pub req: MsgBuf,

    /// Pre-allocated MsgBuf for single-packet responses.
    /// Should accommodate the largest possible response, i.e., [`UdTransport::max_data_in_pkt()`].
    ///
    /// Only used by server, so for client, this is a dummy buffer.
    pub pre_resp_msgbuf: MsgBuf,

    /// Response MsgBuf.
    /// - For server: this is the response generated by the request handler.
    ///   No need for an `Option`, since burying a fake `MsgBuf` is an no-op.
    /// - For client: this is the user-provided response buffer.
    pub resp: MsgBuf,

    /// Packet header for the outbound message.
    /// - For server: header of response.
    /// - For client: header of request.
    pub pkthdr: MsgBuf,

    /// Whether the request is finished.
    /// - For server: this is set to `true` after the request handler returns
    ///   and the response is recorded.
    /// - For client: this is set to `true` after the response is received.
    pub finished: bool,

    /// Whether this SSlot has a living associated request handle.
    pub has_handle: bool,
}

impl SSlot {
    /// Initialize a SSlot.
    #[inline]
    pub fn new(state: &mut RpcInterior, role: SessionRole, req_idx: ReqIdx) -> Self {
        Self {
            req_idx,
            req_type: 0,
            req: MsgBuf::dummy(),
            pre_resp_msgbuf: match role {
                SessionRole::Client => MsgBuf::dummy(),
                SessionRole::Server => state.alloc_msgbuf(UdTransport::max_data_in_pkt()),
            },
            resp: MsgBuf::dummy(),
            pkthdr: state.alloc_pkthdr_buf(),
            finished: false,
            has_handle: false,
        }
    }

    /// Return an immutable reference to the request buffer.
    #[inline(always)]
    pub fn req_buf(&self) -> &MsgBuf {
        &self.req
    }
}
