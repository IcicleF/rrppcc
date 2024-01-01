use std::future::Future;
use std::pin::Pin;

use crate::msgbuf::MsgBuf;
use crate::rpc::Rpc;
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
pub struct Request {
    /// Pointer to the `Rpc` instance that calls this handler function.
    rpc: &'static Rpc,

    /// Pointer to the SSlot of this request.
    sslot: &'static SSlot,
}

impl Request {
    /// Construct a request handle.
    #[inline(always)]
    pub(crate) fn new<'a>(rpc: &'a Rpc, sslot: &'a SSlot) -> Self {
        // SAFETY: `Rpc`s and `SSlots` are unmovable on the heap, so it is
        // safe to make these references into pointers.
        // Also, only request handlers called by an `Rpc` will see the `Request`
        // instance, so from the perspective of the handler, the `Rpc` and `SSlot`
        // instances are always alive, i.e., `'static`.
        Self {
            rpc: unsafe { &*(rpc as *const _) },
            sslot: unsafe { &*(sslot as *const _) },
        }
    }
}

impl Request {
    /// Return the `Rpc` instance that called this handler function.
    #[inline(always)]
    pub fn rpc(&self) -> &Rpc {
        self.rpc
    }

    /// Return the type of this request.
    #[inline(always)]
    pub fn req_type(&self) -> ReqType {
        self.sslot.req_type
    }

    /// Return the request buffer.
    #[inline(always)]
    pub fn req_buf(&self) -> &MsgBuf {
        self.sslot.req_buf()
    }

    /// Return the prepared response buffer.
    ///
    /// This buffer can only accommodate MTU-sized data (usually 4KiB). If you
    /// need larger responses, you should use `Rpc::alloc_msgbuf()`.
    #[inline(always)]
    pub fn resp_buf(&self) -> MsgBuf {
        self.sslot.pre_resp_msgbuf.clone_borrowed()
    }
}
