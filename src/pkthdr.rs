use std::{fmt, hint, mem};

use bitvec::{field::BitField, prelude as bv};
use paste::paste;

use crate::type_alias::*;

macro_rules! impl_accessor {
    ($field:ident, $field_ty:ty, $integral_ty:ty, $lsb:expr, $msb:expr, $setter:tt, $getter:tt) => {
        #[inline(always)]
        pub fn $setter(&mut self, val: $field_ty) {
            self.bits[$lsb..=$msb].store_le::<$integral_ty>(val as $integral_ty);
        }

        #[inline(always)]
        pub fn $getter(&self) -> $field_ty {
            self.bits[$lsb..=$msb].load_le::<$integral_ty>().into()
        }
    };

    ($field:ident, $field_ty:ty, $integral_ty:ty, $lsb:expr, $msb:expr) => {
        paste! {
            impl_accessor!(
                $field,
                $field_ty,
                $integral_ty,
                $lsb,
                $msb,
                [< set_ $field >],
                $field
            );
        }
    };

    ($field:ident, $field_ty:ty, $lsb:expr, $msb:expr) => {
        paste! {
            impl_accessor!(
                $field,
                $field_ty,
                $field_ty,
                $lsb,
                $msb,
                [< set_ $field >],
                $field
            );
        }
    };
}

/// Packet type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PktType {
    /// Request data.
    Req = 0,

    /// Request for response.
    Rfr = 1,

    /// Explicit credit return.
    ExplCR = 2,

    /// Response data.
    Resp = 3,
}

impl From<u8> for PktType {
    fn from(val: u8) -> Self {
        match val {
            0 => Self::Req,
            1 => Self::Rfr,
            2 => Self::ExplCR,
            3 => Self::Resp,

            // SAFETY: only used by `PacketHeader::pkt_type()`, which will only
            // pass 2-bit values to this function.
            _ => unsafe { hint::unreachable_unchecked() },
        }
    }
}

/// Packet header, 16-bytes.
///
/// # Layout
///
/// | Lsb | Msb |     Name     |
/// | --: | --: | ------------ |
/// |   0 |   7 | req_type     |
/// |   8 |  31 | len          |
/// |  32 |  63 | dst_sess_id  |
/// |  64 |  65 | pkt_type     |
/// |  66 |  79 | pkt_idx      |
/// |  80 | 127 | req_idx      |
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct PacketHeader {
    bits: bv::BitArr!(for 128),
}

impl PacketHeader {
    impl_accessor!(req_type, ReqType, 0, 7);
    impl_accessor!(data_len, u32, 8, 31);
    impl_accessor!(dst_sess_id, SessId, 32, 63);
    impl_accessor!(pkt_type, PktType, u8, 64, 65);
    impl_accessor!(pkt_idx, PktIdx, 66, 79);
    impl_accessor!(req_idx, ReqIdx, 80, 127);

    pub fn new(
        req_type: ReqType,
        data_len: u32,
        dst_sess_id: SessId,
        pkt_type: PktType,
        pkt_idx: PktIdx,
        req_idx: ReqIdx,
    ) -> Self {
        let mut this = Self::default();
        this.set_req_type(req_type);
        this.set_data_len(data_len);
        this.set_dst_sess_id(dst_sess_id);
        this.set_pkt_type(pkt_type);
        this.set_pkt_idx(pkt_idx);
        this.set_req_idx(req_idx);
        this
    }
}

impl fmt::Debug for PacketHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PacketHeader")
            .field("req_type", &self.req_type())
            .field("len", &self.data_len())
            .field("dst_sess_id", &self.dst_sess_id())
            .field("pkt_type", &self.pkt_type())
            .field("pkt_idx", &self.pkt_idx())
            .field("req_idx", &self.req_idx())
            .finish()
    }
}

impl Default for PacketHeader {
    fn default() -> Self {
        Self {
            bits: bv::bitarr![0u8; mem::size_of::<PacketHeader>() * 8],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pkthdr_layout() {
        assert_eq!(mem::size_of::<PacketHeader>(), 16);
    }
}
