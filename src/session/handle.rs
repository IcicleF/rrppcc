use crate::rpc::Rpc;
use crate::type_alias::*;

pub struct SessionHandle<'r> {
    /// The RPC instance that owns this session.
    rpc: &'r Rpc,

    /// Session ID.
    sess_id: SessId,
}

impl<'r> SessionHandle<'r> {
    /// Create a new session handle.
    #[inline(always)]
    pub(crate) fn new(rpc: &'r Rpc, sess_id: SessId) -> Self {
        Self { rpc, sess_id }
    }
}

impl<'r> SessionHandle<'r> {
    /// Return the session ID.
    #[inline(always)]
    pub fn sess_id(&self) -> SessId {
        self.sess_id
    }

    /// Return the RPC instance that owns this session.
    #[inline(always)]
    pub fn rpc(&self) -> &'r Rpc {
        self.rpc
    }
}
