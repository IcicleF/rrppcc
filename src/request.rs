use std::future::Future;
use std::pin::Pin;

use crate::msgbuf::MsgBuf;
use crate::session::SSlot;
use crate::type_alias::*;

/// RPC request handler function return type.
pub(crate) type ReqHandlerFuture = Pin<Box<dyn Future<Output = MsgBuf> + Send + Sync + 'static>>;

/// RPC request handler function trait.
///
/// Every handler function take two parameters.
/// The first is a [`Pin<&Rpc>`], which refers to the [`Rpc`] instance that calls
/// this handler function due to a client RPC request.
/// The second is a  [`ReqHandle`] that contains pointers to.
/// The handler should` ` return a future that resolves to the length of the response.
/// The response should be filled in the buffer pointed by `resp_buf`.
pub(crate) type ReqHandler = Box<dyn Fn(Request) -> ReqHandlerFuture + Send + Sync + 'static>;

/// RPC request handle.
#[repr(transparent)]
pub struct Request {
    /// Pointer to the corresponding SSlot.
    sslot: &'static SSlot,
}

impl Request {
    /// Construct a request handle from a SSlot.
    #[inline]
    pub(crate) fn new(sslot: &SSlot) -> Self {
        // SAFETY: the `Request` type contains a 'static SSlot to avoid having generic lifetime
        // parameters, as HRTB is not perfect yet and can be falsely rejected by the compiler.
        //
        // The 'static lifetime is guaranteed by the fact that the `Request` type is only observed
        // by a handler function, which is only called by the RPC instance itself, and the RPC
        // instance cannot get invalidated before the handler function returns.
        // Therefore, from the perspective of the handler function, the `Request` type is 'static.
        Self {
            sslot: unsafe { &*(sslot as *const SSlot) },
        }
    }
}

impl Request {
    /// Return the type of this request.
    #[inline(always)]
    pub fn req_type(&self) -> ReqType {
        self.sslot.req_type
    }

    /// Get the request buffer.
    #[inline(always)]
    pub fn req_buf(&self) -> &MsgBuf {
        self.sslot.req_buf()
    }
}
