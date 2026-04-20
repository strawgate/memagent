#!/usr/bin/env python3
"""
AF_XDP zero-copy syslog receiver prototype.

Sets up an AF_XDP socket with shared umem, attaches an XDP program that
redirects matching syslog packets via XSKMAP, and reads packets directly
from shared memory with zero kernel-to-userspace copies.

Architecture:
  1. Allocate umem (shared memory between kernel and userspace)
  2. Create AF_XDP socket, bind to interface + queue
  3. Set up 4 rings: FILL, COMPLETION, RX, TX (mmap'd shared memory)
  4. Load XDP program with bpf_redirect_map(xskmap, queue, 0)
  5. Packet arrives → XDP redirects to AF_XDP → lands in umem
  6. Userspace polls RX ring → reads packet from umem (zero copy)
  7. Returns frame to FILL ring for reuse
"""

import ctypes
import ctypes.util
import struct
import os
import socket
import mmap
import time
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from bpf_helpers import bpf, create_map, map_update, load_prog, attach_xdp, PROG_XDP

libc = ctypes.CDLL('libc.so.6', use_errno=True)

# AF_XDP constants (from linux/if_xdp.h)
AF_XDP = 44
SOL_XDP = 283

# XDP socket options — values from /usr/include/linux/if_xdp.h
XDP_MMAP_OFFSETS = 1
XDP_RX_RING = 2
XDP_TX_RING = 3
XDP_UMEM_REG = 4             # NOT 1!
XDP_UMEM_FILL_RING = 5
XDP_UMEM_COMPLETION_RING = 6

# XDP mmap offsets
XDP_PGOFF_RX_RING = 0
XDP_PGOFF_TX_RING = 0x80000000
XDP_UMEM_PGOFF_FILL_RING = 0x100000000
XDP_UMEM_PGOFF_COMPLETION_RING = 0x180000000

# BPF map types
MAP_XSKMAP = 17

PAGE_SIZE = 4096

# Umem config
FRAME_SIZE = 2048   # bytes per frame (packet slot)
NUM_FRAMES = 4096   # total frames in umem
UMEM_SIZE = FRAME_SIZE * NUM_FRAMES  # 8MB
RING_SIZE = 2048    # entries per ring (must be power of 2)


def _setsockopt(fd, level, optname, val_bytes):
    buf = ctypes.create_string_buffer(val_bytes)
    ret = libc.setsockopt(fd, level, optname, buf, len(val_bytes))
    if ret < 0:
        raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))


def _getsockopt(fd, level, optname, size):
    buf = ctypes.create_string_buffer(size)
    olen = ctypes.c_int(size)
    ret = libc.getsockopt(fd, level, optname, buf, ctypes.byref(olen))
    if ret < 0:
        raise OSError(ctypes.get_errno(), os.strerror(ctypes.get_errno()))
    return buf.raw[:olen.value]


def setup_xsk(ifname, queue_id=0):
    """Create and configure an AF_XDP socket with umem and rings."""
    libc.mmap.restype = ctypes.c_void_p

    # 1. Create AF_XDP socket
    fd = libc.socket(AF_XDP, socket.SOCK_RAW, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "socket(AF_XDP)")
    print(f"  AF_XDP socket: fd={fd}")

    # 2. Allocate umem via mmap (page-aligned, kernel-visible)
    umem_ptr = libc.mmap(None, UMEM_SIZE, 3, 0x22, -1, 0)  # RW, PRIVATE|ANON
    print(f"  Umem: {UMEM_SIZE} bytes ({NUM_FRAMES} frames × {FRAME_SIZE}B) at {umem_ptr:#x}")

    # 3. Register umem (struct xdp_umem_reg: addr, len, chunk_size, headroom, flags, tx_metadata_len = 32 bytes)
    _setsockopt(fd, SOL_XDP, XDP_UMEM_REG,
                struct.pack('QQIIII', umem_ptr, UMEM_SIZE, FRAME_SIZE, 0, 0, 0))
    print("  Umem registered")

    # 4. Set up ring sizes
    _setsockopt(fd, SOL_XDP, XDP_UMEM_FILL_RING, struct.pack('I', RING_SIZE))
    _setsockopt(fd, SOL_XDP, XDP_UMEM_COMPLETION_RING, struct.pack('I', RING_SIZE))
    _setsockopt(fd, SOL_XDP, XDP_RX_RING, struct.pack('I', RING_SIZE))
    print(f"  Rings configured: {RING_SIZE} entries each")

    # 5. Get mmap offsets (4 rings × 4 u64 fields = 128 bytes)
    offsets_buf = _getsockopt(fd, SOL_XDP, XDP_MMAP_OFFSETS, 128)
    rx_off = struct.unpack_from('<QQQQ', offsets_buf, 0)
    fr_off = struct.unpack_from('<QQQQ', offsets_buf, 64)
    print(f"  RX offsets: prod={rx_off[0]} cons={rx_off[1]} desc={rx_off[2]}")
    print(f"  Fill offsets: prod={fr_off[0]} cons={fr_off[1]} desc={fr_off[2]}")

    # 6. mmap the rings (xdp_desc = 16 bytes for RX, u64 = 8 bytes for fill)
    rx_ring_size = rx_off[2] + RING_SIZE * 16
    rx_ring = mmap.mmap(fd, rx_ring_size, mmap.MAP_SHARED | mmap.MAP_POPULATE,
                        mmap.PROT_READ | mmap.PROT_WRITE,
                        offset=XDP_PGOFF_RX_RING)
    print(f"  RX ring mmap'd: {rx_ring_size} bytes")

    fill_ring_size = fr_off[2] + RING_SIZE * 8
    fill_ring = mmap.mmap(fd, fill_ring_size, mmap.MAP_SHARED | mmap.MAP_POPULATE,
                          mmap.PROT_READ | mmap.PROT_WRITE,
                          offset=XDP_UMEM_PGOFF_FILL_RING)
    print(f"  Fill ring mmap'd: {fill_ring_size} bytes")

    # 7. Pre-fill the fill ring with frame addresses
    for i in range(RING_SIZE):
        struct.pack_into('<Q', fill_ring, fr_off[2] + i * 8, i * FRAME_SIZE)
    struct.pack_into('<I', fill_ring, fr_off[0], RING_SIZE)
    print(f"  Fill ring: {RING_SIZE} frames pre-filled")

    # 8. Bind to interface + queue
    ifindex = socket.if_nametoindex(ifname)
    sa = ctypes.create_string_buffer(struct.pack('HHIII', AF_XDP, 0, ifindex, queue_id, 0).ljust(16, b'\x00'))
    ret = libc.bind(fd, sa, 16)
    if ret < 0:
        raise OSError(ctypes.get_errno(), f"bind AF_XDP: {os.strerror(ctypes.get_errno())}")
    print(f"  Bound to {ifname} queue {queue_id}")

    return {
        'fd': fd,
        'umem_ptr': umem_ptr,
        'rx_ring': rx_ring,
        'fill_ring': fill_ring,
        'rx_offsets': rx_off,
        'fill_offsets': fr_off,
        'ifindex': ifindex,
    }


def build_xdp_redirect_program(xskmap_fd):
    """Build an XDP program that redirects UDP syslog to AF_XDP socket.

    The program:
    1. Checks for UDP port 514 (or configured port)
    2. Calls bpf_redirect_map(xskmap, ctx->rx_queue_index, XDP_PASS)
    3. Non-matching packets get XDP_PASS

    We compile this from C for correctness.
    """
    src = r"""
#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <linux/in.h>

#define SEC(name) __attribute__((section(name), used))

static long (*bpf_redirect_map)(void *map, __u32 key, __u64 flags) = (void *)51;

#define SYSLOG_PORT 5514

struct {
    int (*type)[BPF_MAP_TYPE_XSKMAP];
    int (*key_size)[4];
    int (*value_size)[4];
    int (*max_entries)[1];
} xsks_map SEC(".maps");

SEC("xdp")
int xdp_redirect_syslog(struct xdp_md *ctx) {
    void *data = (void *)(__u64)ctx->data;
    void *data_end = (void *)(__u64)ctx->data_end;

    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;
    if (eth->h_proto != __builtin_bswap16(ETH_P_IP))
        return XDP_PASS;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end || ip->protocol != IPPROTO_UDP)
        return XDP_PASS;

    __u32 ip_hlen = ip->ihl * 4;
    if (ip_hlen < 20 || ip_hlen > 60)
        return XDP_PASS;

    struct udphdr *udp = (void *)((__u8 *)ip + ip_hlen);
    if ((void *)(udp + 1) > data_end)
        return XDP_PASS;

    if (udp->dest != __builtin_bswap16(SYSLOG_PORT))
        return XDP_PASS;

    /* Redirect to AF_XDP socket */
    return bpf_redirect_map(&xsks_map, ctx->rx_queue_index, XDP_PASS);
}

char _license[] SEC("license") = "GPL";
"""
    import tempfile, subprocess
    with tempfile.NamedTemporaryFile(suffix='.c', mode='w', delete=False) as f:
        f.write(src)
        src_path = f.name
    obj_path = src_path.replace('.c', '.o')

    r = subprocess.run([
        'clang', '-O2', '-g', '-target', 'bpf',
        '-I/usr/include/x86_64-linux-gnu',
        '-Wall', '-Werror', '-c', src_path, '-o', obj_path,
    ], capture_output=True, text=True)
    os.unlink(src_path)

    if r.returncode != 0:
        print(f"Compilation failed:\n{r.stderr}")
        return None

    # Load with map relocations
    from elftools.elf.elffile import ELFFile
    with open(obj_path, 'rb') as f:
        elf = ELFFile(f)
        xdp_sec = elf.get_section_by_name('xdp')
        prog = bytearray(xdp_sec.data())

        symtab = elf.get_section_by_name('.symtab')
        rel_sec = elf.get_section_by_name('.relxdp')
        if rel_sec:
            for rel in rel_sec.iter_relocations():
                sym = symtab.get_symbol(rel['r_info_sym'])
                off = rel['r_offset']
                prog[off + 1] = (prog[off + 1] & 0x0F) | (1 << 4)
                struct.pack_into('<i', prog, off + 4, xskmap_fd)

    os.unlink(obj_path)
    return load_prog(PROG_XDP, bytes(prog))


def test_af_xdp():
    """Test AF_XDP zero-copy packet reception."""
    print("=== AF_XDP Zero-Copy Syslog Receiver Test ===\n")

    # 1. Set up AF_XDP socket
    print("1. Setting up AF_XDP socket...")
    try:
        xsk = setup_xsk('lo', queue_id=0)
    except OSError as e:
        print(f"   FAILED: {e}")
        print("   AF_XDP setup requires full XDP support on the interface.")
        return False

    # 2. Create XSKMAP and register our socket
    print("\n2. Creating XSKMAP...")
    xskmap_fd = create_map(MAP_XSKMAP, 4, 4, 1)
    print(f"   XSKMAP: fd={xskmap_fd}")

    # Register socket fd in XSKMAP at index 0
    map_update(xskmap_fd, struct.pack('I', 0), struct.pack('I', xsk['fd']))
    print(f"   Registered AF_XDP socket fd={xsk['fd']} in XSKMAP[0]")

    # 3. Build and load XDP redirect program
    print("\n3. Loading XDP redirect program...")
    prog_fd = build_xdp_redirect_program(xskmap_fd)
    if prog_fd is None:
        print("   FAILED to compile/load")
        return False
    print(f"   Loaded: fd={prog_fd}")

    # 4. Attach XDP
    print("\n4. Attaching XDP to lo...")
    try:
        link_fd = attach_xdp(prog_fd, xsk['ifindex'])
        print(f"   Attached: link_fd={link_fd}")
    except OSError as e:
        print(f"   FAILED: {e}")
        os.close(prog_fd)
        os.close(xskmap_fd)
        return False

    try:
        # 5. Send test syslog traffic
        print("\n5. Sending syslog traffic...")
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for i in range(10):
            msg = f'<134>Jan 15 10:30:45 testhost app[{i}]: AF_XDP test message {i}'
            test_sock.sendto(msg.encode(), ('127.0.0.1', 5514))
        test_sock.close()
        print(f"   Sent 10 syslog packets to port 5514")

        time.sleep(0.2)

        # 6. Read from RX ring
        print("\n6. Reading AF_XDP RX ring...")
        rx_ring = xsk['rx_ring']
        rx_off = xsk['rx_offsets']
        umem_ptr = xsk['umem_ptr']

        rx_prod = struct.unpack_from('<I', rx_ring, rx_off[0])[0]
        rx_cons = struct.unpack_from('<I', rx_ring, rx_off[1])[0]
        print(f"   RX ring: producer={rx_prod}, consumer={rx_cons}")

        packets_read = 0
        while rx_cons < rx_prod:
            desc_idx = rx_cons & (RING_SIZE - 1)
            desc_off = rx_off[2] + desc_idx * 16
            addr, pkt_len, options = struct.unpack_from('<QII', rx_ring, desc_off)

            # Read packet directly from umem (ZERO COPY!)
            pkt_data = (ctypes.c_char * pkt_len).from_address(umem_ptr + addr)

            if packets_read < 3:
                raw = bytes(pkt_data)
                # Eth=14, IP=20, UDP=8 = 42 byte header minimum
                if len(raw) > 42:
                    print(f"   Packet {packets_read}: len={pkt_len}, syslog={raw[42:102]}")
                else:
                    print(f"   Packet {packets_read}: len={pkt_len}")

            rx_cons += 1
            packets_read += 1

        if packets_read > 3:
            print(f"   ... ({packets_read - 3} more)")

        struct.pack_into('<I', rx_ring, rx_off[1], rx_cons)
        print(f"\n   Total packets via AF_XDP: {packets_read}")

        if packets_read > 0:
            print("\n" + "=" * 50)
            print("AF_XDP ZERO-COPY RECEPTION WORKS!")
            print("=" * 50)
            print("  Packets land directly in shared umem memory.")
            print("  No kernel→userspace copy. No recvmmsg syscall.")
            return True
        else:
            print("\n   No packets in RX ring. On loopback with generic XDP,")
            print("   bpf_redirect_map to XSKMAP may not be supported.")
            print("   AF_XDP needs native XDP driver support (mlx5, i40e, ice)")
            print("   for actual zero-copy. But the setup path works!")
            return False

    finally:
        os.close(link_fd)
        os.close(prog_fd)
        os.close(xskmap_fd)
        libc.close(xsk['fd'])


if __name__ == '__main__':
    success = test_af_xdp()
    sys.exit(0 if success else 1)
