use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use rmp_serde as rmps;

use crate::msgbuf::MsgBuf;
use crate::nexus::{SmEvent, SmEventDetails};
use crate::request::Request;
use crate::rpc::Rpc;
use crate::type_alias::*;
use crate::util::{likely::*, thread_check::*};

/// Handle to a session that points to a specific remote `Rpc` endpoint.
#[derive(Clone, Copy)]
pub struct SessionHandle<'r> {
    /// The RPC instance that owns this session.
    rpc: &'r Rpc,

    /// Session ID.
    sess_id: SessId,

    /// Peer Nexus SM URI.
    remote_uri: SocketAddr,

    /// Peer Rpc ID.
    remote_rpc_id: RpcId,
}

unsafe impl Send for SessionHandle<'_> {}
unsafe impl Sync for SessionHandle<'_> {}

impl<'r> SessionHandle<'r> {
    /// Create a new session handle.
    #[inline(always)]
    pub(crate) fn new(
        rpc: &'r Rpc,
        sess_id: SessId,
        remote_uri: SocketAddr,
        remote_rpc_id: RpcId,
    ) -> Self {
        Self {
            rpc,
            sess_id,
            remote_uri,
            remote_rpc_id,
        }
    }
}

impl<'r> SessionHandle<'r> {
    /// Return the session ID.
    #[inline(always)]
    pub fn id(&self) -> SessId {
        self.sess_id
    }

    /// Return the RPC instance that owns this session.
    #[inline(always)]
    pub fn rpc(&self) -> &'r Rpc {
        self.rpc
    }

    /// Return `true` if the session is connected.
    #[inline]
    pub fn is_connected(&self) -> bool {
        do_thread_check(self.rpc);
        self.rpc
            .session_connection_state(self.sess_id)
            .unwrap_or(false)
    }

    /// Connect the session to the remote peer.
    ///
    /// This method returns an awaitable object that resolves to a
    /// boolean value indicating whether the connection is successful.
    pub fn connect<'a>(&'a self) -> impl Future<Output = bool> + 'a
    where
        'r: 'a,
    {
        do_thread_check(self.rpc);
        if likely(!self.is_connected()) {
            // Mark the session as connecting.
            let (cli_ud_ep, cli_sess_rc_ep) = self.rpc.mark_session_connecting(self.sess_id);

            // Send a connect request to the remote peer.
            let event = SmEvent {
                src_rpc_id: self.rpc.id(),
                dst_rpc_id: self.remote_rpc_id,
                details: SmEventDetails::ConnectRequest {
                    cli_uri: self.rpc.nexus().uri(),
                    cli_ud_ep,
                    cli_sess_id: self.sess_id,
                    cli_sess_rc_ep,
                },
            };

            let event_buf = rmps::to_vec(&event).unwrap();
            self.rpc
                .sm_tx
                .send_to(&event_buf, self.remote_uri)
                .expect("failed to send ConnectRequest");
        }

        SessionConnect {
            rpc: self.rpc,
            sess_id: self.sess_id,
        }
    }

    /// Send a request in this session.
    ///
    /// When the return value is resolved, the response buffer will also be resized
    /// to the proper length. Note that truncation may occur if the response buffer
    /// is too small, and there will not be any warning.
    pub fn request<'a>(
        &'a self,
        req_type: ReqType,
        req_msgbuf: &'a MsgBuf,
        resp_msgbuf: &'a mut MsgBuf,
    ) -> Request<'a>
    where
        'r: 'a,
    {
        do_thread_check(self.rpc);
        self.rpc
            .enqueue_request(self.sess_id, req_type, req_msgbuf, resp_msgbuf)
    }
}

/// Session connection awaitable.
struct SessionConnect<'a> {
    /// The `Rpc` instance that owns this session.
    rpc: &'a Rpc,

    /// Session ID.
    sess_id: SessId,
}

impl Future for SessionConnect<'_> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        do_thread_check(self.rpc);

        // Check if the session connection state has already been determined.
        if let Some(result) = self.rpc.session_connection_state(self.sess_id) {
            Poll::Ready(result)
        } else {
            self.rpc.progress();
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

unsafe impl Send for SessionConnect<'_> {}
unsafe impl Sync for SessionConnect<'_> {}
