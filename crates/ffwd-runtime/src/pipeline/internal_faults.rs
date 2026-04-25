//! Internal fault injection hooks for runtime seam testing.
//!
//! Approach A (adopted): failpoint-backed hooks gated by
//! `internal-failpoints`. When disabled, all hooks compile to zero-cost no-ops.

/// Return `true` when checkpoint flush should fail before calling store I/O.
#[inline]
pub(super) fn checkpoint_flush_should_fail() -> bool {
    #[cfg(feature = "internal-failpoints")]
    {
        fail::fail_point!("runtime::pipeline::checkpoint_flush::before_flush", |_| {
            true
        });
    }
    false
}

/// Return `true` when the pipeline should short-circuit before pool submit.
#[inline]
pub(super) fn submit_before_pool_should_hold_and_shutdown() -> bool {
    #[cfg(feature = "internal-failpoints")]
    {
        fail::fail_point!("runtime::pipeline::submit::before_pool_submit", |_| true);
    }
    false
}

/// Return `true` when shutdown should skip draining channel messages.
#[inline]
pub(super) fn shutdown_skip_channel_drain() -> bool {
    #[cfg(feature = "internal-failpoints")]
    {
        fail::fail_point!("runtime::pipeline::run_async::skip_channel_drain", |_| true);
    }
    false
}
