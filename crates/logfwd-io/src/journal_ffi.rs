//! Thin safe wrapper around libsystemd's `sd_journal` API, loaded at runtime
//! via `dlopen`.
//!
//! This avoids any build-time dependency on `libsystemd-dev` and lets the
//! binary run on non-systemd hosts (the journal input will gracefully report
//! that `libsystemd.so.0` is not available).
//!
//! # Safety
//!
//! The [`Journal`](crate::journal_ffi::Journal) handle is `!Send` and `!Sync` because the underlying
//! `sd_journal` object must only be used from the thread that created it.
//! Callers must confine each `Journal` to a single OS thread.

use std::ffi::{CStr, CString};
use std::io;
use std::marker::PhantomData;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;

use libloading::{Library, Symbol};

// ── sd_journal open flags ─────────────────────────────────────────────

/// Only open journal files generated on the local machine.
pub const SD_JOURNAL_LOCAL_ONLY: i32 = 1;
/// Only open system journal.
pub const SD_JOURNAL_SYSTEM: i32 = 4;

// ── sd_journal_wait return values ─────────────────────────────────────

/// Nothing happened during wait.
pub const SD_JOURNAL_NOP: i32 = 0;
/// New entries were appended.
pub const SD_JOURNAL_APPEND: i32 = 1;
/// Journal files were rotated / invalidated.
pub const SD_JOURNAL_INVALIDATE: i32 = 2;

// ── Opaque C type ─────────────────────────────────────────────────────

/// Opaque handle to the C `sd_journal` struct.
enum SdJournal {}

// ── Function pointer type aliases ─────────────────────────────────────

type FnOpen = unsafe extern "C" fn(*mut *mut SdJournal, i32) -> i32;
type FnOpenDirectory = unsafe extern "C" fn(*mut *mut SdJournal, *const libc::c_char, i32) -> i32;
#[allow(dead_code)]
type FnOpenNamespace = unsafe extern "C" fn(*mut *mut SdJournal, *const libc::c_char, i32) -> i32;
type FnClose = unsafe extern "C" fn(*mut SdJournal);

type FnNext = unsafe extern "C" fn(*mut SdJournal) -> i32;
type FnPrevious = unsafe extern "C" fn(*mut SdJournal) -> i32;
type FnSeekHead = unsafe extern "C" fn(*mut SdJournal) -> i32;
type FnSeekTail = unsafe extern "C" fn(*mut SdJournal) -> i32;
type FnSeekCursor = unsafe extern "C" fn(*mut SdJournal, *const libc::c_char) -> i32;

type FnGetCursor = unsafe extern "C" fn(*mut SdJournal, *mut *mut libc::c_char) -> i32;
type FnTestCursor = unsafe extern "C" fn(*mut SdJournal, *const libc::c_char) -> i32;
type FnGetData =
    unsafe extern "C" fn(*mut SdJournal, *const libc::c_char, *mut *const u8, *mut usize) -> i32;
type FnEnumerateData = unsafe extern "C" fn(*mut SdJournal, *mut *const u8, *mut usize) -> i32;
type FnRestartData = unsafe extern "C" fn(*mut SdJournal);
type FnGetRealtimeUsec = unsafe extern "C" fn(*mut SdJournal, *mut u64) -> i32;

type FnAddMatch = unsafe extern "C" fn(*mut SdJournal, *const u8, usize) -> i32;
type FnAddDisjunction = unsafe extern "C" fn(*mut SdJournal) -> i32;
type FnFlushMatches = unsafe extern "C" fn(*mut SdJournal);

type FnWait = unsafe extern "C" fn(*mut SdJournal, u64) -> i32;

// ── LibSystemd: loaded function table ─────────────────────────────────

/// Holds the loaded `libsystemd.so.0` library and resolved function pointers.
///
/// Stored behind an `Arc` so that `Journal` handles can reference-count the
/// library lifetime.
struct LibSystemd {
    _lib: Library,
    open: FnOpen,
    open_directory: FnOpenDirectory,
    /// `sd_journal_open_namespace` — may be `None` on older libsystemd (<245).
    open_namespace: Option<FnOpenNamespace>,
    close: FnClose,
    next: FnNext,
    previous: FnPrevious,
    seek_head: FnSeekHead,
    seek_tail: FnSeekTail,
    seek_cursor: FnSeekCursor,
    get_cursor: FnGetCursor,
    test_cursor: FnTestCursor,
    get_data: FnGetData,
    enumerate_data: FnEnumerateData,
    restart_data: FnRestartData,
    get_realtime_usec: FnGetRealtimeUsec,
    add_match: FnAddMatch,
    add_disjunction: FnAddDisjunction,
    flush_matches: FnFlushMatches,
    wait: FnWait,
}

/// Convert a negative sd_journal return code to `io::Error`.
fn sd_err(ret: i32) -> io::Error {
    io::Error::from_raw_os_error(-ret)
}

impl LibSystemd {
    /// Attempt to load `libsystemd.so.0` from the system's default search path.
    fn load() -> io::Result<Self> {
        // SAFETY: we load a well-known system library. The library name is
        // hard-coded (no user-controlled paths).
        let lib = unsafe { Library::new("libsystemd.so.0") }.map_err(|e| {
            io::Error::other(format!(
                "could not load libsystemd.so.0 — journald native API unavailable: {e}"
            ))
        })?;

        // SAFETY: each symbol name matches the published C ABI of libsystemd.
        // The type aliases above encode the correct signatures.
        unsafe {
            let load = |name: &[u8]| -> io::Result<*mut ()> {
                let sym: Symbol<*mut ()> = lib.get(name).map_err(|e| {
                    io::Error::other(format!(
                        "libsystemd.so.0 missing symbol {}: {e}",
                        String::from_utf8_lossy(name)
                    ))
                })?;
                Ok(*sym)
            };

            macro_rules! sym {
                ($name:literal, $ty:ty) => {{
                    let ptr = load($name)?;
                    std::mem::transmute::<*mut (), $ty>(ptr)
                }};
            }

            // `sd_journal_open_namespace` is optional — only available on
            // systemd >= 245. We try to load it but fall back to None.
            let open_namespace: Option<FnOpenNamespace> = load(b"sd_journal_open_namespace\0")
                .ok()
                .map(|ptr| std::mem::transmute::<*mut (), FnOpenNamespace>(ptr));

            Ok(Self {
                open: sym!(b"sd_journal_open\0", FnOpen),
                open_directory: sym!(b"sd_journal_open_directory\0", FnOpenDirectory),
                open_namespace,
                close: sym!(b"sd_journal_close\0", FnClose),
                next: sym!(b"sd_journal_next\0", FnNext),
                previous: sym!(b"sd_journal_previous\0", FnPrevious),
                seek_head: sym!(b"sd_journal_seek_head\0", FnSeekHead),
                seek_tail: sym!(b"sd_journal_seek_tail\0", FnSeekTail),
                seek_cursor: sym!(b"sd_journal_seek_cursor\0", FnSeekCursor),
                get_cursor: sym!(b"sd_journal_get_cursor\0", FnGetCursor),
                test_cursor: sym!(b"sd_journal_test_cursor\0", FnTestCursor),
                get_data: sym!(b"sd_journal_get_data\0", FnGetData),
                enumerate_data: sym!(b"sd_journal_enumerate_data\0", FnEnumerateData),
                restart_data: sym!(b"sd_journal_restart_data\0", FnRestartData),
                get_realtime_usec: sym!(b"sd_journal_get_realtime_usec\0", FnGetRealtimeUsec),
                add_match: sym!(b"sd_journal_add_match\0", FnAddMatch),
                add_disjunction: sym!(b"sd_journal_add_disjunction\0", FnAddDisjunction),
                flush_matches: sym!(b"sd_journal_flush_matches\0", FnFlushMatches),
                wait: sym!(b"sd_journal_wait\0", FnWait),
                _lib: lib,
            })
        }
    }
}

// ── Journal: safe wrapper ─────────────────────────────────────────────

/// A safe wrapper around an `sd_journal` handle.
///
/// # Thread safety
///
/// `Journal` is intentionally `!Send` and `!Sync`. The underlying
/// `sd_journal` handle must be used exclusively from the thread that opened
/// it.
pub struct Journal {
    handle: *mut SdJournal,
    lib: Arc<LibSystemd>,
    /// Prevent Send + Sync — `Rc` is `!Send + !Sync` on stable Rust.
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // SAFETY: `self.handle` was obtained from `sd_journal_open` (or
            // `sd_journal_open_directory`) and is non-null.
            unsafe {
                (self.lib.close)(self.handle);
            }
        }
    }
}

/// Check whether `libsystemd.so.0` can be loaded on this system.
pub fn is_native_available() -> bool {
    LibSystemd::load().is_ok()
}

impl Journal {
    /// Open the system journal.
    ///
    /// `flags` should be a combination of `SD_JOURNAL_*` constants (e.g.
    /// `SD_JOURNAL_LOCAL_ONLY | SD_JOURNAL_SYSTEM`).
    pub fn open(flags: i32) -> io::Result<Self> {
        let lib = Arc::new(LibSystemd::load()?);
        let mut handle: *mut SdJournal = ptr::null_mut();
        // SAFETY: `sd_journal_open` writes a valid handle pointer on success.
        // We pass a pointer to a local `handle` variable, which is valid for
        // the duration of this call.
        let ret = unsafe { (lib.open)(std::ptr::addr_of_mut!(handle), flags) };
        if ret < 0 {
            return Err(sd_err(ret));
        }
        Ok(Self {
            handle,
            lib,
            _not_send: PhantomData,
        })
    }

    /// Open a journal from a specific directory.
    pub fn open_directory(path: &str, flags: i32) -> io::Result<Self> {
        let lib = Arc::new(LibSystemd::load()?);
        let c_path = CString::new(path)
            .map_err(|_| io::Error::other("journal directory path contains null byte"))?;
        let mut handle: *mut SdJournal = ptr::null_mut();
        // SAFETY: `c_path` is a valid NUL-terminated C string. `handle` pointer
        // is valid for the duration of this call.
        let ret =
            unsafe { (lib.open_directory)(std::ptr::addr_of_mut!(handle), c_path.as_ptr(), flags) };
        if ret < 0 {
            return Err(sd_err(ret));
        }
        Ok(Self {
            handle,
            lib,
            _not_send: PhantomData,
        })
    }

    /// Open a journal scoped to a specific namespace.
    ///
    /// Requires systemd >= 245 (`sd_journal_open_namespace`). Returns an error
    /// if the symbol is not available in the loaded `libsystemd.so.0`.
    pub fn open_namespace(namespace: &str, flags: i32) -> io::Result<Self> {
        let lib = Arc::new(LibSystemd::load()?);
        let fn_open_ns = lib.open_namespace.ok_or_else(|| {
            io::Error::other("sd_journal_open_namespace not available (requires systemd >= 245)")
        })?;
        let c_ns = CString::new(namespace)
            .map_err(|_| io::Error::other("journal namespace contains null byte"))?;
        let mut handle: *mut SdJournal = ptr::null_mut();
        // SAFETY: `c_ns` is a valid NUL-terminated C string. `handle` pointer
        // is valid for the duration of this call.
        let ret = unsafe { fn_open_ns(std::ptr::addr_of_mut!(handle), c_ns.as_ptr(), flags) };
        if ret < 0 {
            return Err(sd_err(ret));
        }
        Ok(Self {
            handle,
            lib,
            _not_send: PhantomData,
        })
    }

    // ── Navigation ────────────────────────────────────────────────────

    /// Advance to the next journal entry.
    ///
    /// Returns `true` if an entry is available, `false` if the end was reached.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> io::Result<bool> {
        // SAFETY: `self.handle` is a valid, non-null sd_journal pointer
        // obtained from a successful `sd_journal_open*` call.
        let ret = unsafe { (self.lib.next)(self.handle) };
        if ret < 0 {
            Err(sd_err(ret))
        } else {
            Ok(ret > 0)
        }
    }

    /// Move to the previous journal entry.
    pub fn previous(&mut self) -> io::Result<bool> {
        // SAFETY: `self.handle` is a valid sd_journal pointer.
        let ret = unsafe { (self.lib.previous)(self.handle) };
        if ret < 0 {
            Err(sd_err(ret))
        } else {
            Ok(ret > 0)
        }
    }

    /// Seek to the head (oldest entry).
    pub fn seek_head(&mut self) -> io::Result<()> {
        // SAFETY: `self.handle` is a valid sd_journal pointer.
        let ret = unsafe { (self.lib.seek_head)(self.handle) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(()) }
    }

    /// Seek to the tail (newest entry).
    pub fn seek_tail(&mut self) -> io::Result<()> {
        // SAFETY: `self.handle` is a valid sd_journal pointer.
        let ret = unsafe { (self.lib.seek_tail)(self.handle) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(()) }
    }

    /// Seek to the entry after the given cursor.
    pub fn seek_cursor(&mut self, cursor: &str) -> io::Result<()> {
        let c_cursor =
            CString::new(cursor).map_err(|_| io::Error::other("cursor contains null byte"))?;
        // SAFETY: `self.handle` is valid. `c_cursor` is a valid NUL-terminated
        // C string that remains live for the duration of this call.
        let ret = unsafe { (self.lib.seek_cursor)(self.handle, c_cursor.as_ptr()) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(()) }
    }

    // ── Data access ───────────────────────────────────────────────────

    /// Get the cursor string for the current entry.
    ///
    /// The returned string uniquely identifies this entry and can be used with
    /// [`seek_cursor`](Self::seek_cursor) to resume reading.
    pub fn cursor(&mut self) -> io::Result<String> {
        let mut raw: *mut libc::c_char = ptr::null_mut();
        // SAFETY: `sd_journal_get_cursor` allocates a NUL-terminated string on
        // success and writes its address into `raw`.
        let ret = unsafe { (self.lib.get_cursor)(self.handle, std::ptr::addr_of_mut!(raw)) };
        if ret < 0 {
            return Err(sd_err(ret));
        }
        // SAFETY: on success, `raw` is a valid NUL-terminated malloc'd string.
        let cursor = unsafe { CStr::from_ptr(raw) }
            .to_string_lossy()
            .into_owned();
        // SAFETY: `raw` was allocated by libsystemd via malloc; we must free it.
        unsafe {
            libc::free(raw.cast());
        }
        Ok(cursor)
    }

    /// Test whether the current journal position matches the given cursor.
    ///
    /// Returns `true` if the cursor matches, `false` otherwise. Useful after
    /// [`seek_cursor`](Self::seek_cursor) to determine whether the cursor entry
    /// still exists or if the journal positioned on the next closest entry.
    pub fn test_cursor(&mut self, cursor: &str) -> io::Result<bool> {
        let c_cursor =
            CString::new(cursor).map_err(|_| io::Error::other("cursor contains null byte"))?;
        // SAFETY: `self.handle` is valid. `c_cursor` is a valid NUL-terminated
        // C string that remains live for the duration of this call.
        let ret = unsafe { (self.lib.test_cursor)(self.handle, c_cursor.as_ptr()) };
        if ret < 0 {
            Err(sd_err(ret))
        } else {
            Ok(ret > 0)
        }
    }

    /// Get the value of a specific field from the current entry.
    ///
    /// Returns the raw `FIELD=value` bytes. Returns `None` if the field is not
    /// present in the current entry.
    pub fn get_data(&mut self, field: &str) -> io::Result<Option<&[u8]>> {
        let c_field =
            CString::new(field).map_err(|_| io::Error::other("field name contains null byte"))?;
        let mut data: *const u8 = ptr::null();
        let mut len: usize = 0;
        // SAFETY: `sd_journal_get_data` writes a pointer to internal buffer
        // memory into `data` and the byte count into `len`. The `c_field`
        // pointer is valid for the duration of this call.
        let ret = unsafe {
            (self.lib.get_data)(
                self.handle,
                c_field.as_ptr(),
                std::ptr::addr_of_mut!(data),
                std::ptr::addr_of_mut!(len),
            )
        };
        if ret == -libc::ENOENT {
            return Ok(None);
        }
        if ret < 0 {
            return Err(sd_err(ret));
        }
        // SAFETY: `data` points to `len` bytes of valid memory in the journal's
        // internal buffer. The slice is valid until the next `get_data`,
        // `enumerate_data`, or navigation call on this handle.
        Ok(Some(unsafe { std::slice::from_raw_parts(data, len) }))
    }

    /// Get the realtime timestamp (microseconds since epoch) for the current entry.
    pub fn realtime_usec(&mut self) -> io::Result<u64> {
        let mut usec: u64 = 0;
        // SAFETY: `self.handle` is valid. `usec` is a valid local variable.
        let ret =
            unsafe { (self.lib.get_realtime_usec)(self.handle, std::ptr::addr_of_mut!(usec)) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(usec) }
    }

    /// Begin iterating all fields in the current entry.
    pub fn restart_data(&mut self) {
        // SAFETY: `sd_journal_restart_data` has no failure mode and only
        // resets internal iteration state on a valid handle.
        unsafe {
            (self.lib.restart_data)(self.handle);
        }
    }

    /// Get the next `FIELD=value` pair from the current entry.
    ///
    /// Returns `None` when all fields have been enumerated. Call
    /// [`restart_data`](Self::restart_data) to start over.
    pub fn enumerate_data(&mut self) -> io::Result<Option<&[u8]>> {
        let mut data: *const u8 = ptr::null();
        let mut len: usize = 0;
        // SAFETY: `sd_journal_enumerate_data` writes the next field's pointer
        // and length. Both output variables are valid stack locals.
        let ret = unsafe {
            (self.lib.enumerate_data)(
                self.handle,
                std::ptr::addr_of_mut!(data),
                std::ptr::addr_of_mut!(len),
            )
        };
        if ret == 0 {
            // No more fields.
            return Ok(None);
        }
        if ret < 0 {
            return Err(sd_err(ret));
        }
        // SAFETY: `data` points to `len` bytes in the journal's internal
        // buffer, valid until the next data/navigation call.
        Ok(Some(unsafe { std::slice::from_raw_parts(data, len) }))
    }

    // ── Filtering ─────────────────────────────────────────────────────

    /// Add a match filter. The `data` should be in `FIELD=value` format.
    ///
    /// Multiple matches on the same field are OR'd. Use
    /// [`add_disjunction`](Self::add_disjunction) to separate match groups.
    pub fn add_match(&mut self, data: &[u8]) -> io::Result<()> {
        // SAFETY: `data` slice pointer and length are valid for the call
        // duration. `self.handle` is valid.
        let ret = unsafe { (self.lib.add_match)(self.handle, data.as_ptr(), data.len()) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(()) }
    }

    /// Insert an OR between match groups.
    pub fn add_disjunction(&mut self) -> io::Result<()> {
        // SAFETY: `self.handle` is a valid sd_journal pointer.
        let ret = unsafe { (self.lib.add_disjunction)(self.handle) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(()) }
    }

    /// Remove all match filters.
    pub fn flush_matches(&mut self) {
        // SAFETY: `sd_journal_flush_matches` has no failure mode.
        unsafe {
            (self.lib.flush_matches)(self.handle);
        }
    }

    // ── Waiting ───────────────────────────────────────────────────────

    /// Block until the journal changes or the timeout expires.
    ///
    /// `timeout_usec` is in microseconds. Pass `u64::MAX` for infinite wait.
    ///
    /// Returns one of `SD_JOURNAL_NOP`, `SD_JOURNAL_APPEND`, or
    /// `SD_JOURNAL_INVALIDATE`.
    pub fn wait(&mut self, timeout_usec: u64) -> io::Result<i32> {
        // SAFETY: `self.handle` is a valid sd_journal pointer.
        let ret = unsafe { (self.lib.wait)(self.handle, timeout_usec) };
        if ret < 0 { Err(sd_err(ret)) } else { Ok(ret) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_native_available_returns_bool() {
        // On CI without libsystemd-dev this may be false; that's fine.
        let available = is_native_available();
        // Just ensure it doesn't panic.
        eprintln!("native journald API available: {available}");
    }

    #[test]
    fn open_and_seek_if_available() {
        if !is_native_available() {
            eprintln!("skipping: libsystemd.so.0 not available");
            return;
        }
        let mut journal =
            Journal::open(SD_JOURNAL_LOCAL_ONLY | SD_JOURNAL_SYSTEM).expect("open failed");
        journal.seek_tail().expect("seek_tail failed");
        // After seek_tail we need previous() to land on the last entry.
        let has_entry = journal.previous().expect("previous failed");
        if has_entry {
            let cursor = journal.cursor().expect("cursor failed");
            assert!(!cursor.is_empty(), "cursor should not be empty");
        }
    }

    #[test]
    fn enumerate_fields_if_available() {
        if !is_native_available() {
            eprintln!("skipping: libsystemd.so.0 not available");
            return;
        }
        let mut journal =
            Journal::open(SD_JOURNAL_LOCAL_ONLY | SD_JOURNAL_SYSTEM).expect("open failed");
        journal.seek_tail().expect("seek_tail failed");
        if !journal.previous().expect("previous") {
            eprintln!("no journal entries, skipping");
            return;
        }
        journal.restart_data();
        let mut count = 0;
        while let Ok(Some(field)) = journal.enumerate_data() {
            // Each field is FIELD=value
            assert!(
                memchr::memchr(b'=', field).is_some(),
                "field should contain '='"
            );
            count += 1;
        }
        assert!(count > 0, "entry should have at least one field");
    }
}
