#!/usr/bin/env python3
"""
XDP Syslog Filter — integration test.

Compiles the XDP C program, loads it with map relocations via pyelftools,
attaches to loopback, sends syslog traffic, and verifies filtering + events.
"""

import struct
import os
import socket
import sysconfig
import time
import subprocess
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from bpf_helpers import (
    bpf, create_map, map_update, map_lookup_percpu, load_prog, attach_xdp,
    read_ringbuf, MAP_ARRAY, MAP_PERCPU_ARRAY, MAP_RINGBUF, PROG_XDP,
)
from elftools.elf.elffile import ELFFile


# ----- Map definitions (must match xdp_syslog.c) -----

MAP_DEFS = {
    'events': {'type': MAP_RINGBUF,       'key': 0, 'val': 0, 'max': 4 * 1024 * 1024},
    'config': {'type': MAP_ARRAY,          'key': 4, 'val': 8, 'max': 4},
    'stats':  {'type': MAP_PERCPU_ARRAY,   'key': 4, 'val': 8, 'max': 8},
}

# Config indices
CFG_SEVERITY_THRESHOLD = 0
CFG_SYSLOG_PORT = 1

# Stats indices
STAT_NAMES = ['packets_seen', 'syslog_matched', 'severity_dropped',
              'ringbuf_full', 'forwarded', 'parse_failed']

SEV_NAMES = ['EMERG', 'ALERT', 'CRIT', 'ERR', 'WARN', 'NOTICE', 'INFO', 'DEBUG']


def _bpf_include_path():
    """Return the platform-specific include path for kernel headers."""
    # Try multiarch path first (e.g. /usr/include/x86_64-linux-gnu)
    multiarch = sysconfig.get_config_var('MULTIARCH')
    if multiarch:
        p = f'/usr/include/{multiarch}'
        if os.path.isdir(p):
            return p
    # Fallback to generic
    return '/usr/include'


def load_xdp_from_elf(obj_path, map_fds):
    """Load XDP program from ELF, applying map fd relocations."""
    with open(obj_path, 'rb') as f:
        elf = ELFFile(f)

        xdp_sec = elf.get_section_by_name('xdp')
        assert xdp_sec, "No 'xdp' section"
        prog = bytearray(xdp_sec.data())

        symtab = elf.get_section_by_name('.symtab')

        rel_sec = elf.get_section_by_name('.relxdp')
        if rel_sec:
            for rel in rel_sec.iter_relocations():
                sym = symtab.get_symbol(rel['r_info_sym'])
                map_name = sym.name
                if map_name not in map_fds:
                    raise ValueError(f"Unknown map: {map_name}")
                off = rel['r_offset']
                prog[off + 1] = (prog[off + 1] & 0x0F) | (1 << 4)
                struct.pack_into('<i', prog, off + 4, map_fds[map_name])

    return load_prog(PROG_XDP, bytes(prog))


def parse_syslog_event(data):
    """Parse a syslog_event struct from ring buffer."""
    if len(data) < 16:
        return None
    src_ip, src_port, facility, severity, msg_len, captured_len, pri_len, _ = \
        struct.unpack_from('<IHBBHHHH', data, 0)
    if captured_len > len(data) - 16:
        return None
    payload = data[16:16 + captured_len]
    return {
        'src_ip': socket.inet_ntoa(struct.pack('<I', src_ip)),
        'src_port': src_port,
        'facility': facility,
        'severity': severity,
        'msg_len': msg_len,
        'captured_len': captured_len,
        'pri_len': pri_len,
        'payload': payload,
    }


def compile_xdp():
    src = os.path.join(os.path.dirname(__file__), 'xdp_syslog.c')
    obj = os.path.join(os.path.dirname(__file__), 'xdp_syslog.o')
    r = subprocess.run([
        'clang', '-O2', '-g', '-target', 'bpf',
        f'-I{_bpf_include_path()}',
        '-Wall', '-Werror', '-c', src, '-o', obj,
    ], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"BPF compilation failed:\n{r.stderr}")
    return obj


def send_syslog(messages, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for msg in messages:
        sock.sendto(msg.encode() if isinstance(msg, str) else msg, ('127.0.0.1', port))
    sock.close()


def test_xdp_syslog():
    print("=== XDP Syslog Filter Test ===\n")

    print("1. Compiling XDP program...")
    obj_path = compile_xdp()

    maps = {}
    prog_fd = None
    link_fd = None
    try:
        # Create maps
        print("2. Creating BPF maps...")
        maps = {name: create_map(d['type'], d['key'], d['val'], d['max'])
                for name, d in MAP_DEFS.items()}

        # Configure: severity <= WARN (4), port 5514
        TEST_PORT = 5514
        map_update(maps['config'], struct.pack('I', CFG_SEVERITY_THRESHOLD), struct.pack('Q', 4))
        map_update(maps['config'], struct.pack('I', CFG_SYSLOG_PORT), struct.pack('Q', TEST_PORT))

        # Load and attach
        print("3. Loading and attaching XDP...")
        prog_fd = load_xdp_from_elf(obj_path, maps)
        link_fd = attach_xdp(prog_fd, socket.if_nametoindex('lo'))
        print(f"   Attached to lo (prog_fd={prog_fd}, link_fd={link_fd})")

        # Send test traffic: 8 severities × 2 rounds + noise
        print("4. Sending syslog traffic...")
        messages = []
        for sev in range(8):
            pri = (16 << 3) | sev  # facility=local0
            messages.append(f'<{pri}>Jan 15 10:30:45 testhost app[1234]: severity={SEV_NAMES[sev]} msg {sev}')

        send_syslog(messages, TEST_PORT)
        send_syslog(messages, TEST_PORT)

        # Non-syslog noise: invalid format on the listened port, and valid
        # syslog on wrong port (verifies kernel routing, not XDP filtering)
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(b'not syslog', ('127.0.0.1', TEST_PORT))
        sock.sendto(b'<134>wrong port', ('127.0.0.1', TEST_PORT + 1))
        sock.close()

        time.sleep(0.2)

        # Read ring buffer
        print("5. Reading ring buffer events...")
        events = read_ringbuf(maps['events'], 4 * 1024 * 1024)
        parsed = [p for e in events if (p := parse_syslog_event(e))]

        for p in parsed[:3]:
            print(f"   [{SEV_NAMES[p['severity']]}] fac={p['facility']} "
                  f"payload={p['payload'][:60]}")
        if len(parsed) > 3:
            print(f"   ... ({len(parsed) - 3} more)")

        # Verify severity filter: only sev 0-4 (EMERG..WARN), × 2 sends = 10
        print("\n6. Assertions...")
        assert len(parsed) == 10, f"Expected 10 events, got {len(parsed)}"
        assert max(p['severity'] for p in parsed) <= 4, "Severity filter failed"
        for p in parsed:
            assert p['facility'] == 16
            assert f"severity={SEV_NAMES[p['severity']]}" in p['payload'].decode()
        print("   Severity filter: PASS")
        print("   Pre-parsed metadata: PASS")

        # Stats
        print("\n7. Stats...")
        for i, name in enumerate(STAT_NAMES):
            val = map_lookup_percpu(maps['stats'], struct.pack('I', i))
            if val > 0:
                print(f"   {name}: {val}")

        forwarded = map_lookup_percpu(maps['stats'], struct.pack('I', 4))
        dropped = map_lookup_percpu(maps['stats'], struct.pack('I', 2))
        assert forwarded == 10, f"Forwarded {forwarded} != 10"
        assert dropped == 6, f"Dropped {dropped} != 6"
        print("   Stats: PASS")

    finally:
        if link_fd is not None:
            os.close(link_fd)
        if prog_fd is not None:
            os.close(prog_fd)
        for fd in maps.values():
            os.close(fd)

    print(f"\n{'=' * 50}")
    print("ALL TESTS PASSED")
    print(f"{'=' * 50}")


if __name__ == '__main__':
    test_xdp_syslog()
