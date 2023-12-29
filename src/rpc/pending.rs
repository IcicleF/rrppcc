use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::msgbuf::MsgBuf;
use crate::request::ReqHandlerFuture;
use crate::type_alias::*;

/// Pending RPC request handler.
pub(super) struct PendingHandler {
    /// Session ID.
    pub sess_id: SessId,

    /// Slot ID.
    pub slot_idx: usize,

    /// RPC handler body.
    handler: ReqHandlerFuture,
}

impl PendingHandler {
    /// Create a new pending handler.
    #[inline]
    pub fn new(sess_id: SessId, slot_idx: usize, handler: ReqHandlerFuture) -> Self {
        Self {
            sess_id,
            slot_idx,
            handler,
        }
    }
}

impl Future for PendingHandler {
    type Output = MsgBuf;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.handler.as_mut().poll(cx)
    }
}
