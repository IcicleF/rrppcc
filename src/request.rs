//! Define `Request`, an awaitable object that resolves to an RPC response.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use quanta::Instant;

use crate::msgbuf::MsgBuf;
use crate::rpc::Rpc;
use crate::type_alias::*;
use crate::util::thread_check::do_thread_check;

/// An awaitable object that represents an incompleted RPC request.
///
/// Resolves to nothing when the request is completed.
/// The user should find the response in the buffer they provided when sending the request.
pub struct Request<'a> {
    /// Pointer to the `Rpc` instance that this request belongs to.
    rpc: &'a Rpc,

    /// Pointer to the `MsgBuf` that the user designated for the response.
    resp_buf: &'a mut MsgBuf,

    /// Session ID.
    sess_id: SessId,

    /// SSlot index.
    sslot_idx: usize,

    /// Request index.
    req_idx: ReqIdx,

    /// Request start time.
    start_time: Instant,

    /// Indicates whether this request has been aborted.
    /// If `true`, polling this request will always return `Poll::Pending`.
    aborted: bool,
}

unsafe impl Send for Request<'_> {}
unsafe impl Sync for Request<'_> {}

impl<'a> Request<'a> {
    /// Create a new request.
    #[inline(always)]
    pub(crate) fn new(
        rpc: &'a Rpc,
        resp_buf: &'a mut MsgBuf,
        sess_id: SessId,
        sslot_idx: usize,
        req_idx: ReqIdx,
    ) -> Self {
        Self {
            rpc,
            resp_buf,
            sess_id,
            sslot_idx,
            req_idx,
            start_time: Instant::recent(),
            aborted: false,
        }
    }
}

impl Request<'_> {
    /// Retransmit the request if the response is not received within 20ms.
    pub const RETX_TIMEOUT: Duration = Duration::from_millis(20);
}

impl<'a> Future for Request<'a> {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        do_thread_check(self.rpc);

        // If the request has been aborted, always return `Poll::Pending`.
        if self.aborted {
            return Poll::Pending;
        }

        // If the request has finished, return the response.
        let need_re_tx = Instant::recent() - self.start_time > Self::RETX_TIMEOUT;
        if self.rpc.check_request_completion(
            (self.sess_id, self.sslot_idx, self.req_idx),
            self.resp_buf,
            need_re_tx,
        ) {
            return Poll::Ready(());
        }

        // Otherwise, try to make some progress.
        self.rpc.progress();
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
