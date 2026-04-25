#![no_std]

pub const MAX_DATA: usize = 4096;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PipeEvent {
    pub pid: u32,
    pub tgid: u32,
    pub cgroup_id: u64,
    pub len: u32,
    pub captured: u32,
    pub data: [u8; MAX_DATA],
}
