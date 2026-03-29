"""
Shared BPF helpers for eBPF prototype tests.

Thin wrappers around the BPF syscall for map operations, program loading,
ring buffer consumption, and XDP attachment. Used by both the sandbox
capability test and the XDP syslog filter test.
"""

import ctypes
import struct
import os
import mmap

BPF_SYSCALL = 321
PAGE_SIZE = os.sysconf('SC_PAGE_SIZE')

# BPF commands
BPF_MAP_CREATE = 0
BPF_MAP_LOOKUP_ELEM = 1
BPF_MAP_UPDATE_ELEM = 2
BPF_PROG_LOAD = 5
BPF_LINK_CREATE = 28

# Common map types
MAP_HASH = 1
MAP_ARRAY = 2
MAP_PERCPU_ARRAY = 6
MAP_RINGBUF = 27

# Program types
PROG_SOCKET_FILTER = 1
PROG_XDP = 6

# Attach types
BPF_XDP = 37

libc = ctypes.CDLL('libc.so.6', use_errno=True)
_syscall = libc.syscall
_syscall.restype = ctypes.c_long


def bpf(cmd, attr_bytes, size=256):
    """Raw BPF syscall. Returns fd or raises OSError."""
    buf = ctypes.create_string_buffer(attr_bytes.ljust(size, b'\x00'))
    r = _syscall(BPF_SYSCALL, cmd, buf, size)
    if r < 0:
        raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
    return r


def create_map(map_type, key_size, val_size, max_entries):
    return bpf(BPF_MAP_CREATE, struct.pack('IIII', map_type, key_size, val_size, max_entries))


def map_update(fd, key_bytes, val_bytes):
    k = ctypes.create_string_buffer(key_bytes)
    v = ctypes.create_string_buffer(val_bytes)
    bpf(BPF_MAP_UPDATE_ELEM,
        struct.pack('IQQI', fd, ctypes.addressof(k), ctypes.addressof(v), 0))


def map_lookup(fd, key_bytes, val_size=8):
    k = ctypes.create_string_buffer(key_bytes)
    v = ctypes.create_string_buffer(val_size)
    bpf(BPF_MAP_LOOKUP_ELEM,
        struct.pack('IQQI', fd, ctypes.addressof(k), ctypes.addressof(v), 0))
    return v.raw


def map_lookup_percpu(fd, key_bytes):
    """Lookup a per-CPU value and return the sum across all CPUs.

    Returns 0 only for ENOENT (key not found). Other errors propagate.
    """
    ncpu = os.cpu_count() or 1
    k = ctypes.create_string_buffer(key_bytes)
    v = ctypes.create_string_buffer(8 * ncpu)
    try:
        bpf(BPF_MAP_LOOKUP_ELEM,
            struct.pack('IQQI', fd, ctypes.addressof(k), ctypes.addressof(v), 0))
        return sum(struct.unpack_from('<Q', v.raw, i * 8)[0] for i in range(ncpu))
    except OSError as e:
        import errno as errno_mod
        if e.errno == errno_mod.ENOENT:
            return 0
        raise


def load_prog(prog_type, insns_bytes, log_size=512 * 1024):
    """Load a BPF program. Returns fd."""
    insn_cnt = len(insns_bytes) // 8
    insns_buf = ctypes.create_string_buffer(insns_bytes)
    license_buf = ctypes.create_string_buffer(b'GPL\x00')
    log_buf = ctypes.create_string_buffer(log_size) if log_size else None

    attr = struct.pack('IIQQIIQ', prog_type, insn_cnt,
                       ctypes.addressof(insns_buf),
                       ctypes.addressof(license_buf),
                       1 if log_buf else 0,
                       log_size if log_buf else 0,
                       ctypes.addressof(log_buf) if log_buf else 0)
    try:
        return bpf(BPF_PROG_LOAD, attr)
    except OSError as e:
        if log_buf:
            log = log_buf.value.decode('ascii', errors='replace')
            raise OSError(e.errno, f"{e.strerror}\nVerifier:\n{log[:3000]}") from None
        raise


def attach_xdp(prog_fd, ifindex):
    """Attach XDP program via BPF_LINK_CREATE. Returns link fd."""
    return bpf(BPF_LINK_CREATE, struct.pack('IIII', prog_fd, ifindex, BPF_XDP, 0))


def read_ringbuf(rb_fd, rb_size):
    """Read all available events from a BPF ring buffer.

    Ring buffer mmap layout (3 sections):
      offset 0:          consumer page (consumer_pos u64, rw)
      offset PAGE_SIZE:  producer page (producer_pos u64, ro)
      offset 2*PAGE:     data pages (2x ring size for wrap-around, ro)
    """
    consumer_mm = mmap.mmap(rb_fd, PAGE_SIZE, mmap.MAP_SHARED,
                            mmap.PROT_READ | mmap.PROT_WRITE, offset=0)
    producer_mm = mmap.mmap(rb_fd, PAGE_SIZE, mmap.MAP_SHARED,
                            mmap.PROT_READ, offset=PAGE_SIZE)
    data_mm = mmap.mmap(rb_fd, 2 * rb_size, mmap.MAP_SHARED,
                        mmap.PROT_READ, offset=2 * PAGE_SIZE)

    try:
        consumer_pos = struct.unpack('<Q', consumer_mm[:8])[0]
        producer_pos = struct.unpack('<Q', producer_mm[:8])[0]

        events = []
        pos = consumer_pos
        while pos < producer_pos:
            off = pos % rb_size
            hdr = struct.unpack_from('<I', data_mm, off)[0]
            entry_len = hdr & 0x0FFFFFFF
            busy = (hdr >> 31) & 1
            if busy or entry_len == 0 or entry_len > rb_size:
                break
            aligned = (entry_len + 7) & ~7
            discard = (hdr >> 30) & 1
            if not discard:
                data_off = (off + 8) % rb_size
                events.append(data_mm[data_off:data_off + entry_len])
            pos += 8 + aligned

        # Only advance consumer after successful processing
        consumer_mm[:8] = struct.pack('<Q', pos)
        return events
    finally:
        consumer_mm.close()
        producer_mm.close()
        data_mm.close()
