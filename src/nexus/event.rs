use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

use crossbeam::queue::SegQueue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::type_alias::*;

/// Reasons for refusing a connection request.
#[derive(Debug, Clone, Copy, Error, Serialize, Deserialize)]
pub(crate) enum ConnectRefuseReason {
    #[error("invalid Rpc ID")]
    InvalidRpcId,

    #[error("session limit exceeded")]
    SessionLimitExceeded,
}

/// Details of a [`SmEvent`].
#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum SmEventDetails {
    /// A request sent from remote peer to connect to a local [`Rpc`].
    ConnectRequest {
        cli_uri: SocketAddr,
        cli_ep: Vec<u8>,
        cli_sess_id: SessId,
    },

    /// Positive response of a `ConnectRequest`.
    ConnectAcknowledge {
        cli_sess_id: SessId,
        svr_ep: Vec<u8>,
        svr_sess_id: SessId,
    },

    /// Negative response of a `ConnectRequest`.
    ConnectRefuse {
        cli_sess_id: SessId,
        reason: ConnectRefuseReason,
    },

    /// Disconnect request from remote peer.
    Disconnect,
}

impl fmt::Debug for SmEventDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct OpaqueEpHelper(usize);
        impl fmt::Debug for OpaqueEpHelper {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "<{} bytes opaque>", self.0)
            }
        }

        match self {
            SmEventDetails::ConnectRequest {
                cli_uri,
                cli_ep,
                cli_sess_id,
            } => f
                .debug_struct("ConnectRequest")
                .field("cli_uri", cli_uri)
                .field("cli_ep", &OpaqueEpHelper(cli_ep.len()))
                .field("cli_sess_id", cli_sess_id)
                .finish(),
            SmEventDetails::ConnectAcknowledge {
                cli_sess_id,
                svr_ep,
                svr_sess_id,
            } => f
                .debug_struct("ConnectAcknowledge")
                .field("cli_sess_id", cli_sess_id)
                .field("svr_ep", &OpaqueEpHelper(svr_ep.len()))
                .field("svr_sess_id", svr_sess_id)
                .finish(),
            SmEventDetails::ConnectRefuse {
                cli_sess_id,
                reason,
            } => f
                .debug_struct("ConnectRefuse")
                .field("cli_sess_id", cli_sess_id)
                .field("reason", reason)
                .finish(),
            SmEventDetails::Disconnect => f.debug_struct("Disconnect").finish(),
        }
    }
}

/// Event triggered by the [`Nexus`] and handled by the [`Rpc`] instances.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SmEvent {
    /// The ID of the [`Rpc`] instance that this event comes from.
    pub(crate) src_rpc_id: RpcId,

    /// The ID of the [`Rpc`] instance that this event is targeted at.
    pub(crate) dst_rpc_id: RpcId,

    /// The details of this event, including its type and necessary parameters.
    pub(crate) details: SmEventDetails,
}

/// Event sender.
pub(crate) struct SmEventTx(Arc<SegQueue<SmEvent>>);

impl SmEventTx {
    /// Send an event.
    pub(crate) fn send(&self, event: SmEvent) {
        self.0.push(event);
    }
}

/// Event receiver.
pub(crate) struct SmEventRx(Arc<SegQueue<SmEvent>>);

impl SmEventRx {
    /// Receive an event.
    pub(crate) fn recv(&self) -> Option<SmEvent> {
        self.0.pop()
    }

    /// Returns `true` if there is no event in the queue.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Create a pair of event sender and receiver.
pub(crate) fn sm_event_channel() -> (SmEventTx, SmEventRx) {
    let queue = Arc::new(SegQueue::new());
    (SmEventTx(queue.clone()), SmEventRx(queue))
}
