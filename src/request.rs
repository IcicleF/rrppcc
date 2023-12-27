use std::future::Future;
use std::pin::Pin;

use crate::msgbuf::MsgBuf;

/// RPC request handler function return type.
pub(crate) type ReqHandlerFuture = Pin<Box<dyn Future<Output = MsgBuf> + Send + Sync + 'static>>;

/// RPC request handler function trait.
///
/// Every handler function take two parameters.
/// The first is a [`Pin<&Rpc>`], which refers to the [`Rpc`] instance that calls
/// this handler function due to a client RPC request.
/// The second is a  [`ReqHandle`] that contains pointers to.
/// The handler should return a future that resolves to the length of the response.
/// The response should be filled in the buffer pointed by `resp_buf`.
pub(crate) type ReqHandler = Box<dyn Fn(Request) -> ReqHandlerFuture + Send + Sync + 'static>;

/// RPC request handle.
pub struct Request {}
