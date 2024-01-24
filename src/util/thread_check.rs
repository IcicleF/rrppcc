use crate::rpc::Rpc;

#[cfg(not(feature = "no_thread_checks"))]
#[inline(always)]
pub(crate) fn do_thread_check(rpc: &Rpc) {
    #[inline(never)]
    #[cold]
    fn do_thread_check_fail() {
        panic!("Rpc must not be used on a different thread than it was created on");
    }

    if std::thread::current().id() != rpc.thread_id {
        do_thread_check_fail();
    }
}

#[cfg(feature = "no_thread_checks")]
#[inline(always)]
pub(crate) fn do_thread_check(_rpc: &Rpc) {}
