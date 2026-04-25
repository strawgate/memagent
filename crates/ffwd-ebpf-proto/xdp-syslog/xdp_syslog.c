/*
 * XDP Syslog Filter — non-destructive tap for log forwarding
 *
 * Inspects UDP syslog traffic, parses <priority> in-kernel, filters by
 * severity threshold, and copies compact events to a ring buffer.
 * Always returns XDP_PASS — existing syslog receivers keep working.
 *
 * XDP does only 3-5 bytes of parsing per packet (the <NNN> prefix),
 * just enough to decide whether to forward. Userspace gets pre-parsed
 * facility+severity for free and SIMD-parses the rest in batch.
 */

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <linux/in.h>

#define SEC(name) __attribute__((section(name), used))

static long (*bpf_ringbuf_output)(void *map, void *data, __u64 size, __u64 flags) = (void *)130;
static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;

/* ----- Constants ----- */

#define SYSLOG_PORT        514
#define PAYLOAD_PREFIX_LEN 128   /* enough for syslog header fields */

/* Config map indices */
#define CFG_SEVERITY_THRESHOLD 0 /* forward messages with severity <= this */
#define CFG_SYSLOG_PORT        1 /* UDP port to match */
#define CFG_MAX                4

/* Stats map indices */
#define STAT_PACKETS_SEEN      0
#define STAT_SYSLOG_MATCHED    1
#define STAT_SEVERITY_DROPPED  2
#define STAT_RINGBUF_FULL      3
#define STAT_FORWARDED         4
#define STAT_PARSE_FAILED      5
#define STAT_MAX               8

/* ----- Event struct sent to userspace (fits in BPF 512-byte stack) ----- */

struct syslog_event {
    __u32 src_ip;                       /*  0: source IP (network order) */
    __u16 src_port;                     /*  4: source port (host order) */
    __u8  facility;                     /*  6: syslog facility (0-23) */
    __u8  severity;                     /*  7: syslog severity (0-7) */
    __u16 msg_len;                      /*  8: full UDP payload length */
    __u16 captured_len;                 /* 10: bytes in data[] */
    __u16 pri_len;                      /* 12: length of <NNN> prefix */
    __u16 _pad;                         /* 14: alignment */
    __u8  data[PAYLOAD_PREFIX_LEN];     /* 16: payload prefix */
};                                      /* total: 144 bytes */

/* ----- BPF Maps ----- */

struct {
    int (*type)[BPF_MAP_TYPE_RINGBUF];
    int (*max_entries)[4 * 1024 * 1024];
} events SEC(".maps");

struct {
    int (*type)[BPF_MAP_TYPE_ARRAY];
    int (*key_size)[4];
    int (*value_size)[8];
    int (*max_entries)[CFG_MAX];
} config SEC(".maps");

struct {
    int (*type)[BPF_MAP_TYPE_PERCPU_ARRAY];
    int (*key_size)[4];
    int (*value_size)[8];
    int (*max_entries)[STAT_MAX];
} stats SEC(".maps");

/* ----- Helpers ----- */

static __attribute__((always_inline))
void inc_stat(__u32 idx) {
    __u64 *val = bpf_map_lookup_elem(&stats, &idx);
    if (val)
        __sync_fetch_and_add(val, 1);
}

static __attribute__((always_inline))
__u64 get_config(__u32 idx, __u64 default_val) {
    __u64 *val = bpf_map_lookup_elem(&config, &idx);
    return val ? *val : default_val;
}

/*
 * Parse syslog priority from "<NNN>" prefix.
 * Returns priority (0-191) or -1 on failure.
 */
static __attribute__((always_inline))
int parse_priority(const __u8 *data, const __u8 *end, __u16 *pri_len) {
    if (data + 3 > end || data[0] != '<')
        return -1;

    /* Require at least one digit — reject "<>" */
    if (data[1] < '0' || data[1] > '9') return -1;
    int pri = data[1] - '0';

    if (data + 3 > end) return -1;
    if (data[2] == '>') { *pri_len = 3; return (pri <= 191) ? pri : -1; }
    if (data[2] < '0' || data[2] > '9') return -1;
    pri = pri * 10 + (data[2] - '0');

    if (data + 4 > end) return -1;
    if (data[3] == '>') { *pri_len = 4; return (pri <= 191) ? pri : -1; }
    if (data[3] < '0' || data[3] > '9') return -1;
    pri = pri * 10 + (data[3] - '0');

    if (data + 5 > end) return -1;
    if (data[4] == '>') { *pri_len = 5; return (pri <= 191) ? pri : -1; }
    return -1;
}

/* ----- XDP Program ----- */

SEC("xdp")
int xdp_syslog_filter(struct xdp_md *ctx) {
    void *data = (void *)(__u64)ctx->data;
    void *data_end = (void *)(__u64)ctx->data_end;

    inc_stat(STAT_PACKETS_SEEN);

    /* Parse Ethernet → IP → UDP */
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return XDP_PASS;
    if (eth->h_proto != __builtin_bswap16(ETH_P_IP))
        return XDP_PASS;

    struct iphdr *ip = (void *)(eth + 1);
    if ((void *)(ip + 1) > data_end || ip->protocol != IPPROTO_UDP)
        return XDP_PASS;

    /* Skip IP fragments — only first fragment has a valid UDP header */
    if (ip->frag_off & __builtin_bswap16(0x1FFF | 0x2000))
        return XDP_PASS;

    __u32 ip_hlen = ip->ihl * 4;
    if (ip_hlen < 20 || ip_hlen > 60)
        return XDP_PASS;

    struct udphdr *udp = (void *)((__u8 *)ip + ip_hlen);
    if ((void *)(udp + 1) > data_end)
        return XDP_PASS;

    __u16 port = (__u16)get_config(CFG_SYSLOG_PORT, SYSLOG_PORT);
    if (udp->dest != __builtin_bswap16(port))
        return XDP_PASS;

    inc_stat(STAT_SYSLOG_MATCHED);

    /* Syslog payload — clamp to bytes actually present in packet */
    __u8 *payload = (__u8 *)(udp + 1);
    __u16 udp_len = __builtin_bswap16(udp->len);
    if (udp_len < 8)
        return XDP_PASS;
    __u16 payload_len = udp_len - 8;
    __u16 available = (__u16)((__u8 *)data_end - payload);
    if (payload_len > available)
        payload_len = available;

    /* Parse <priority> — the only in-kernel parsing we do */
    __u16 pri_len = 0;
    int priority = parse_priority(payload, data_end, &pri_len);
    if (priority < 0) {
        inc_stat(STAT_PARSE_FAILED);
        return XDP_PASS;
    }

    __u8 severity = priority & 0x7;

    /* Severity filter (configurable at runtime via map, no reload needed) */
    if (severity > (__u8)get_config(CFG_SEVERITY_THRESHOLD, 7)) {
        inc_stat(STAT_SEVERITY_DROPPED);
        return XDP_PASS;
    }

    /* Build compact event: pre-parsed metadata + payload prefix.
     * Zero-init to avoid leaking stack data if copy loop exits early. */
    struct syslog_event evt = {};
    evt.src_ip = ip->saddr;
    evt.src_port = __builtin_bswap16(udp->source);
    evt.facility = priority >> 3;
    evt.severity = severity;
    evt.msg_len = payload_len;
    evt.pri_len = pri_len;
    evt._pad = 0;

    __u16 cap = payload_len;
    if (cap > PAYLOAD_PREFIX_LEN)
        cap = PAYLOAD_PREFIX_LEN;
    evt.captured_len = cap;

    /* Bounded copy for BPF verifier */
    for (__u16 i = 0; i < PAYLOAD_PREFIX_LEN; i++) {
        if (i >= cap)
            break;
        if (payload + i + 1 > (__u8 *)data_end)
            break;
        evt.data[i] = payload[i];
    }

    /* Send only header + captured bytes (not the full 144-byte struct) */
    __u32 evt_size = 16 + cap;
    if (evt_size > sizeof(evt))
        evt_size = sizeof(evt);

    if (bpf_ringbuf_output(&events, &evt, evt_size, 0) < 0) {
        inc_stat(STAT_RINGBUF_FULL);
        return XDP_PASS;
    }

    inc_stat(STAT_FORWARDED);
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
