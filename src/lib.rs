//! A simple **unsound** RDMA RPC library.

mod handler;
mod msgbuf;
mod nexus;
mod pkthdr;
mod request;
mod rpc;
mod session;
mod transport;
pub mod type_alias;
mod util;

pub use self::msgbuf::MsgBuf;
pub use self::nexus::Nexus;
pub use self::request::Request;
pub use self::rpc::Rpc;
pub use self::session::SessionHandle as Session;

pub use self::handler::RequestHandle;

#[cfg(test)]
mod tests;
