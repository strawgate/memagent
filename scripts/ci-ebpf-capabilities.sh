#!/usr/bin/env bash
set -euo pipefail

out_dir="${1:-artifacts/ebpf-capabilities}"
mkdir -p "${out_dir}"

status_file="${out_dir}/status.tsv"
summary_file="${out_dir}/summary.md"
env_file="${out_dir}/summary.env"

: > "${status_file}"

has_cmd() {
    command -v "$1" >/dev/null 2>&1
}

is_bpffs_mounted() {
    if has_cmd mountpoint; then
        mountpoint -q /sys/fs/bpf
        return
    fi
    if has_cmd findmnt; then
        findmnt -n /sys/fs/bpf >/dev/null 2>&1
        return
    fi
    return 1
}

record_check() {
    local key="$1"
    shift
    if "$@"; then
        printf "%s\t1\n" "${key}" >> "${status_file}"
        return 0
    fi
    printf "%s\t0\n" "${key}" >> "${status_file}"
    return 1
}

record_cmd() {
    local key="$1"
    shift
    local out_path="${out_dir}/${key}.txt"
    if "$@" > "${out_path}" 2>&1; then
        printf "%s\t1\n" "${key}" >> "${status_file}"
        return 0
    fi
    printf "%s\t0\n" "${key}" >> "${status_file}"
    return 1
}

status_of() {
    local key="$1"
    awk -F '\t' -v k="${key}" '$1 == k { print $2 }' "${status_file}" | tail -n1
}

yes_no() {
    if [[ "$1" == "1" ]]; then
        echo "yes"
        return
    fi
    echo "no"
}

record_check "cgroup_v2" test -f /sys/fs/cgroup/cgroup.controllers || true
record_check "btf_vmlinux" test -r /sys/kernel/btf/vmlinux || true
record_check "bpffs_mountpoint_exists" test -d /sys/fs/bpf || true
record_check "bpffs_already_mounted" is_bpffs_mounted || true
record_check "bpftool_installed" command -v bpftool >/dev/null 2>&1 || true
if sudo -n true >/dev/null 2>&1; then
    record_check "sudo_available" true || true
else
    record_check "sudo_available" false || true
fi

if [[ "$(status_of bpftool_installed)" == "1" ]]; then
    record_cmd "bpftool_version" bpftool version || true
    record_cmd "bpftool_feature_unprivileged" bpftool feature probe kernel unprivileged || true

    if [[ "$(status_of sudo_available)" == "1" ]]; then
        record_cmd "bpftool_feature_privileged" sudo bpftool feature probe kernel || true
        if ! is_bpffs_mounted; then
            sudo mount -t bpf bpf /sys/fs/bpf >/dev/null 2>&1 || true
        fi
        record_check "bpffs_mounted_after_probe" is_bpffs_mounted || true
        record_cmd \
            "bpftool_map_create" \
            sudo bpftool map create /sys/fs/bpf/ffwd_ci_probe_map type hash key 4 value 8 entries 16 name ffwd_ci_probe_map \
            || true
        sudo rm -f /sys/fs/bpf/ffwd_ci_probe_map >/dev/null 2>&1 || true
    fi
fi

kernel="$(uname -r)"
unprivileged_setting="unknown"
if [[ -r /proc/sys/kernel/unprivileged_bpf_disabled ]]; then
    unprivileged_setting="$(cat /proc/sys/kernel/unprivileged_bpf_disabled)"
fi

cat > "${env_file}" <<EOF
KERNEL_RELEASE=${kernel}
UNPRIVILEGED_BPF_DISABLED=${unprivileged_setting}
CGROUP_V2=$(yes_no "$(status_of cgroup_v2)")
BTF_VMLINUX=$(yes_no "$(status_of btf_vmlinux)")
BPFFS_EXISTS=$(yes_no "$(status_of bpffs_mountpoint_exists)")
BPFFS_ALREADY_MOUNTED=$(yes_no "$(status_of bpffs_already_mounted)")
BPFFS_MOUNTED_AFTER_PROBE=$(yes_no "$(status_of bpffs_mounted_after_probe)")
BPFTOOL_INSTALLED=$(yes_no "$(status_of bpftool_installed)")
BPFTOOL_UNPRIV_FEATURE_PROBE=$(yes_no "$(status_of bpftool_feature_unprivileged)")
BPFTOOL_PRIV_FEATURE_PROBE=$(yes_no "$(status_of bpftool_feature_privileged)")
BPFTOOL_MAP_CREATE=$(yes_no "$(status_of bpftool_map_create)")
SUDO_AVAILABLE=$(yes_no "$(status_of sudo_available)")
EOF

cat > "${summary_file}" <<EOF
## eBPF capability snapshot

| Check | Result |
|---|---|
| Kernel release | \`${kernel}\` |
| \`/proc/sys/kernel/unprivileged_bpf_disabled\` | \`${unprivileged_setting}\` |
| cgroup v2 available | $(yes_no "$(status_of cgroup_v2)") |
| BTF vmlinux available | $(yes_no "$(status_of btf_vmlinux)") |
| bpffs mountpoint exists | $(yes_no "$(status_of bpffs_mountpoint_exists)") |
| bpffs already mounted | $(yes_no "$(status_of bpffs_already_mounted)") |
| bpffs mounted after probe | $(yes_no "$(status_of bpffs_mounted_after_probe)") |
| \`bpftool\` installed | $(yes_no "$(status_of bpftool_installed)") |
| Unprivileged feature probe | $(yes_no "$(status_of bpftool_feature_unprivileged)") |
| Privileged feature probe | $(yes_no "$(status_of bpftool_feature_privileged)") |
| Privileged map create smoke test | $(yes_no "$(status_of bpftool_map_create)") |
| Passwordless sudo available | $(yes_no "$(status_of sudo_available)") |

### Artifacts

- \`status.tsv\`: machine-readable pass/fail checks.
- \`bpftool_*.txt\`: raw probe output and errors.
- \`summary.env\`: key/value summary for automation.
EOF

cat "${summary_file}"
