#!/usr/bin/env python3
"""
eBPF sandbox capability test.

Tests the full BPF lifecycle in the current environment:
1. BPF map creation (hash, array, ringbuf)
2. BPF program load (socket filter)
3. BPF program attach to socket
4. Packet counting via BPF map
5. Ring buffer event production and consumption via mmap

This validates the data path that pipe-capture will use,
even though kprobes are not available in this sandbox.
"""

import ctypes
import struct
import os
import socket
import mmap
import time

BPF_SYSCALL = 321

# BPF commands
BPF_MAP_CREATE = 0
BPF_MAP_LOOKUP_ELEM = 1
BPF_MAP_UPDATE_ELEM = 2
BPF_PROG_LOAD = 5

# Map types
BPF_MAP_TYPE_HASH = 1
BPF_MAP_TYPE_ARRAY = 2
BPF_MAP_TYPE_RINGBUF = 27

BPF_PROG_TYPE_SOCKET_FILTER = 1
SO_ATTACH_BPF = 50

libc = ctypes.CDLL('libc.so.6', use_errno=True)
syscall = libc.syscall
syscall.restype = ctypes.c_long

PAGE_SIZE = os.sysconf('SC_PAGE_SIZE')


def bpf_cmd(cmd, attr_bytes, size=256):
    buf = ctypes.create_string_buffer(attr_bytes.ljust(size, b'\x00'))
    r = syscall(BPF_SYSCALL, cmd, buf, size)
    if r < 0:
        errno = ctypes.get_errno()
        raise OSError(errno, f"bpf({cmd}): {os.strerror(errno)}")
    return r


def insn(op, dst, src, off, imm):
    """Encode a single BPF instruction (8 bytes)."""
    return struct.pack('<BBhi', op, (src << 4) | dst, off, imm)


def load_prog(prog_bytes, log=False):
    """Load a BPF socket filter program. Returns fd."""
    insn_cnt = len(prog_bytes) // 8
    insns_buf = ctypes.create_string_buffer(prog_bytes)
    license_buf = ctypes.create_string_buffer(b'GPL\x00')
    if log:
        log_buf = ctypes.create_string_buffer(65536)
        attr = struct.pack('IIQQIIQ', BPF_PROG_TYPE_SOCKET_FILTER, insn_cnt,
                           ctypes.addressof(insns_buf), ctypes.addressof(license_buf),
                           1, 65536, ctypes.addressof(log_buf))
    else:
        attr = struct.pack('IIQQIIQ', BPF_PROG_TYPE_SOCKET_FILTER, insn_cnt,
                           ctypes.addressof(insns_buf), ctypes.addressof(license_buf),
                           0, 0, 0)
    return bpf_cmd(BPF_PROG_LOAD, attr)


def map_lookup(map_fd, key_bytes):
    """Lookup a value in a BPF map."""
    key_buf = ctypes.create_string_buffer(key_bytes)
    val_buf = ctypes.create_string_buffer(8)
    attr = struct.pack('IQQI', map_fd,
                       ctypes.addressof(key_buf), ctypes.addressof(val_buf), 0)
    bpf_cmd(BPF_MAP_LOOKUP_ELEM, attr)
    return val_buf.raw


def send_and_drain(raw_sock, num_packets=10, port=55570):
    """Send UDP packets on loopback and drain them through the BPF-attached socket."""
    t = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for i in range(num_packets):
        t.sendto(f'pkt-{i}'.encode(), ('127.0.0.1', port))
    recv = 0
    while True:
        try:
            raw_sock.recv(4096)
            recv += 1
        except socket.timeout:
            break
    t.close()
    return recv


# ---------- Tests ----------

def test_map_create():
    """Test creating various BPF map types."""
    print("=== Test: BPF Map Creation ===")
    for mtype, name, args in [
        (BPF_MAP_TYPE_HASH, 'HASH', (4, 4, 16)),
        (BPF_MAP_TYPE_ARRAY, 'ARRAY', (4, 4, 16)),
        (BPF_MAP_TYPE_RINGBUF, 'RINGBUF', (0, 0, 256 * 1024)),
    ]:
        fd = bpf_cmd(BPF_MAP_CREATE, struct.pack('IIII', mtype, *args))
        os.close(fd)
        print(f"  {name}: OK")
    print("  PASSED\n")


def test_prog_load():
    """Test loading a minimal BPF program."""
    print("=== Test: BPF Program Load ===")
    # r0 = *(u32*)(r1 + 0); exit  (return skb->len = accept all)
    prog = bytes([
        0x61, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ])
    fd = load_prog(prog)
    os.close(fd)
    print("  Socket filter loaded: OK")
    print("  PASSED\n")


def test_socket_attach():
    """Test attaching BPF to a socket and filtering packets."""
    print("=== Test: Socket Attach + Packet Capture ===")
    prog = bytes([
        0x61, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x95, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ])
    prog_fd = load_prog(prog)

    raw_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
    raw_sock.bind(('lo', 0))
    raw_sock.setsockopt(socket.SOL_SOCKET, SO_ATTACH_BPF, struct.pack('I', prog_fd))
    raw_sock.settimeout(0.5)

    recv = send_and_drain(raw_sock, 5, 55571)
    assert recv > 0, f"Expected packets, got {recv}"
    print(f"  Received {recv} packets through BPF filter")

    raw_sock.close()
    os.close(prog_fd)
    print("  PASSED\n")


def test_map_counter():
    """Test BPF program that increments a shared map counter per packet."""
    print("=== Test: BPF Map Counter ===")

    # Create array map with 1 entry (u64)
    map_fd = bpf_cmd(BPF_MAP_CREATE, struct.pack('IIII', BPF_MAP_TYPE_ARRAY, 4, 8, 1))

    # Initialize to 0
    key_buf = ctypes.create_string_buffer(struct.pack('I', 0))
    val_buf = ctypes.create_string_buffer(struct.pack('Q', 0))
    bpf_cmd(BPF_MAP_UPDATE_ELEM,
            struct.pack('IQQI', map_fd, ctypes.addressof(key_buf),
                        ctypes.addressof(val_buf), 0))

    # BPF program: lookup counter[0], increment, store back, return skb->len
    prog = b''
    prog += insn(0xbf, 6, 1, 0, 0)                           # r6 = ctx
    prog += insn(0xb7, 0, 0, 0, 0)                            # r0 = 0
    prog += struct.pack('<BBhi', 0x63, (0 << 4) | 10, -4, 0)  # *(u32*)(fp-4) = 0

    # map_lookup_elem(map, &key)
    prog += struct.pack('<BBhi', 0x18, (1 << 4) | 1, 0, map_fd)
    prog += struct.pack('<BBhi', 0x00, 0, 0, 0)
    prog += insn(0xbf, 2, 10, 0, 0)                           # r2 = fp
    prog += insn(0x07, 2, 0, 0, -4)                           # r2 = fp - 4
    prog += insn(0x85, 0, 0, 0, 1)                            # call map_lookup_elem

    prog += insn(0x15, 0, 0, 2, 0)                            # if r0 == 0 goto exit
    prog += struct.pack('<BBhi', 0x79, (0 << 4) | 1, 0, 0)    # r1 = *(u64*)(r0)
    prog += insn(0x07, 1, 0, 0, 1)                            # r1 += 1
    prog += struct.pack('<BBhi', 0x7b, (1 << 4) | 0, 0, 0)    # *(u64*)(r0) = r1

    # return skb->len
    prog += struct.pack('<BBhi', 0x61, (6 << 4) | 0, 0, 0)
    prog += insn(0x95, 0, 0, 0, 0)

    prog_fd = load_prog(prog)

    raw_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
    raw_sock.bind(('lo', 0))
    raw_sock.setsockopt(socket.SOL_SOCKET, SO_ATTACH_BPF, struct.pack('I', prog_fd))
    raw_sock.settimeout(0.5)

    recv = send_and_drain(raw_sock, 10, 55572)
    counter = struct.unpack('<Q', map_lookup(map_fd, struct.pack('I', 0)))[0]

    print(f"  Packets received: {recv}")
    print(f"  BPF counter value: {counter}")
    assert counter > 0, "BPF counter should be > 0"
    assert counter == recv, f"Counter ({counter}) should match received ({recv})"

    raw_sock.close()
    os.close(prog_fd)
    os.close(map_fd)
    print("  PASSED\n")
    return counter


def test_ringbuf_data_path():
    """Test ring buffer event flow: BPF -> ringbuf_output -> mmap -> userspace.

    This is the core data path that pipe-capture uses for sending captured
    write data from kernel to userspace.

    Key discovery: the ring buffer mmap has 3 sections:
      offset 0:            consumer page (consumer_pos, u64, rw)
      offset PAGE_SIZE:    producer page (producer_pos, u64, ro)
      offset 2*PAGE_SIZE:  data pages (2x ring size for wrap-around, ro)
    """
    print("=== Test: Ring Buffer Data Path ===")

    rb_size = 256 * 1024
    rb_fd = bpf_cmd(BPF_MAP_CREATE,
                    struct.pack('IIII', BPF_MAP_TYPE_RINGBUF, 0, 0, rb_size))
    print(f"  Ring buffer created: fd={rb_fd}")

    # BPF program: write 8-byte event {0xDEAD, 0xBEEF} to ringbuf per packet
    # Uses bpf_ringbuf_output (helper 130) which copies stack data to ringbuf
    prog = b''
    prog += insn(0xbf, 6, 1, 0, 0)                            # r6 = ctx (skb)

    # Prepare event on stack: {0xDEAD, 0xBEEF}
    prog += insn(0xb7, 0, 0, 0, 0xDEAD)
    prog += struct.pack('<BBhi', 0x63, (0 << 4) | 10, -16, 0)  # *(u32*)(fp-16) = 0xDEAD
    prog += insn(0xb7, 0, 0, 0, 0xBEEF)
    prog += struct.pack('<BBhi', 0x63, (0 << 4) | 10, -12, 0)  # *(u32*)(fp-12) = 0xBEEF

    # bpf_ringbuf_output(&map, fp-16, 8, 0)
    prog += struct.pack('<BBhi', 0x18, (1 << 4) | 1, 0, rb_fd)
    prog += struct.pack('<BBhi', 0x00, 0, 0, 0)
    prog += insn(0xbf, 2, 10, 0, 0)                            # r2 = fp
    prog += insn(0x07, 2, 0, 0, -16)                           # r2 = fp - 16
    prog += insn(0xb7, 3, 0, 0, 8)                             # r3 = 8 (size)
    prog += insn(0xb7, 4, 0, 0, 0)                             # r4 = 0 (flags)
    prog += insn(0x85, 0, 0, 0, 130)                           # call bpf_ringbuf_output

    # return skb->len (accept packet)
    prog += struct.pack('<BBhi', 0x61, (6 << 4) | 0, 0, 0)
    prog += insn(0x95, 0, 0, 0, 0)

    prog_fd = load_prog(prog)
    print(f"  Program loaded: fd={prog_fd}")

    # Attach to AF_PACKET on loopback
    raw_sock = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(3))
    raw_sock.bind(('lo', 0))
    raw_sock.setsockopt(socket.SOL_SOCKET, SO_ATTACH_BPF, struct.pack('I', prog_fd))
    raw_sock.settimeout(0.5)

    # Send packets and drain
    recv = send_and_drain(raw_sock, 10, 55573)
    time.sleep(0.05)

    # mmap ring buffer with correct 3-section layout
    consumer_mm = mmap.mmap(rb_fd, PAGE_SIZE, mmap.MAP_SHARED,
                            mmap.PROT_READ | mmap.PROT_WRITE, offset=0)
    producer_mm = mmap.mmap(rb_fd, PAGE_SIZE, mmap.MAP_SHARED,
                            mmap.PROT_READ, offset=PAGE_SIZE)
    data_mm = mmap.mmap(rb_fd, 2 * rb_size, mmap.MAP_SHARED,
                        mmap.PROT_READ, offset=2 * PAGE_SIZE)

    consumer_mm.seek(0)
    consumer_pos = struct.unpack('<Q', consumer_mm.read(8))[0]
    producer_mm.seek(0)
    producer_pos = struct.unpack('<Q', producer_mm.read(8))[0]

    print(f"  consumer_pos={consumer_pos}, producer_pos={producer_pos}")

    # Parse ring buffer entries
    events = 0
    pos = consumer_pos
    while pos < producer_pos:
        off = pos % rb_size
        data_mm.seek(off)
        hdr = struct.unpack('<I', data_mm.read(4))[0]

        entry_len = hdr & 0x0FFFFFFF
        busy = (hdr >> 31) & 1
        discard = (hdr >> 30) & 1

        if busy or entry_len == 0:
            break

        aligned = (entry_len + 7) & ~7

        if not discard:
            data_mm.seek((off + 8) % rb_size)
            ev = data_mm.read(entry_len)
            if len(ev) >= 8:
                a, b = struct.unpack('<II', ev[:8])
                if events < 3:
                    print(f"    Event {events}: 0x{a:X}, 0x{b:X}")
                events += 1

        pos += 8 + aligned

    if events > 3:
        print(f"    ... ({events - 3} more)")

    # Advance consumer position
    consumer_mm.seek(0)
    consumer_mm.write(struct.pack('<Q', pos))

    print(f"  Packets received: {recv}")
    print(f"  Ring buffer events: {events}")

    consumer_mm.close()
    producer_mm.close()
    data_mm.close()
    raw_sock.close()
    os.close(prog_fd)
    os.close(rb_fd)

    assert events > 0, "Expected ring buffer events"
    assert events == recv, f"Events ({events}) should match packets ({recv})"
    print("  PASSED\n")
    return events


def test_kernel_config():
    """Report kernel config relevant to eBPF."""
    print("=== Test: Kernel Configuration ===")

    configs = [
        ('CONFIG_BPF_SYSCALL', True),
        ('CONFIG_BPF_JIT', False),
        ('CONFIG_KPROBES', False),
        ('CONFIG_FTRACE', False),
        ('CONFIG_CGROUP_BPF', True),
        ('CONFIG_BPF_STREAM_PARSER', True),
        ('CONFIG_XDP_SOCKETS', True),
    ]

    try:
        import gzip
        with gzip.open('/proc/config.gz', 'rt') as f:
            config = f.read()

        for name, expected in configs:
            is_set = f'{name}=y' in config or f'{name}=m' in config
            marker = 'OK' if is_set == expected else 'NOTE'
            status = 'y' if f'{name}=y' in config else ('m' if f'{name}=m' in config else 'not set')
            print(f"  [{marker}] {name} = {status}")
    except Exception as e:
        print(f"  Could not read /proc/config.gz: {e}")

    btf = os.path.exists('/sys/kernel/btf/vmlinux')
    print(f"  [{'OK' if btf else 'NOTE'}] BTF vmlinux: {'present' if btf else 'absent'}")
    print()


def main():
    print("eBPF Sandbox Capability Test")
    print(f"Kernel: {os.uname().release}")
    print(f"UID: {os.getuid()}")
    print()

    test_kernel_config()
    test_map_create()
    test_prog_load()
    test_socket_attach()
    counter = test_map_counter()
    rb_events = test_ringbuf_data_path()

    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  BPF syscall:        WORKS")
    print(f"  Map creation:       WORKS (hash, array, ringbuf)")
    print(f"  Program load:       WORKS (socket filter)")
    print(f"  Socket attach:      WORKS (AF_PACKET)")
    print(f"  Map counter:        WORKS ({counter} increments)")
    print(f"  Ring buffer:        WORKS ({rb_events} events via bpf_ringbuf_output)")
    print(f"  Write interception: NOT POSSIBLE (CONFIG_KPROBES not set)")
    print()
    print("The full BPF data path (load -> attach -> execute -> ringbuf -> mmap)")
    print("works in this sandbox. Write interception requires CONFIG_KPROBES=y.")
    print()
    print("Ring buffer mmap layout (discovered):")
    print("  offset 0:          consumer page (consumer_pos, rw)")
    print("  offset PAGE_SIZE:  producer page (producer_pos, ro)")
    print("  offset 2*PAGE:     data pages (2x ring size, ro)")


if __name__ == '__main__':
    main()
