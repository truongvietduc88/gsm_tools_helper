use std::path::Path;
use sysinfo::{Disks, System};

#[derive(Debug, Clone, Copy)]
pub enum DiskKind {
    Hdd,
    Ssd,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct HwInfo {
    pub logical_cpus: usize,
    pub total_ram_mb: u64,
    pub disk_kind: DiskKind,
}

fn cpu_logical() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

fn total_ram_mb() -> u64 {
    let mut sys = System::new();
    sys.refresh_memory();
    sys.total_memory() / 1024 // KiB -> MB
}

fn drive_root(p: &Path) -> Option<String> {
    let s = p.to_string_lossy();
    if s.len() >= 3
        && s.as_bytes()[1] == b':'
        && (s.as_bytes()[2] == b'\\' || s.as_bytes()[2] == b'/')
    {
        Some(format!("{}:\\", &s[0..1].to_ascii_uppercase()))
    } else {
        None
    }
}

pub fn disk_kind_for_path(p: &Path) -> DiskKind {
    let root = match drive_root(p) {
        Some(r) => r,
        None => return DiskKind::Unknown,
    };

    let disks = Disks::new_with_refreshed_list();
    for d in disks.iter() {
        let mp = d.mount_point().to_string_lossy().to_string();
        if mp.to_ascii_uppercase().starts_with(&root) {
            return match d.kind() {
                sysinfo::DiskKind::HDD => DiskKind::Hdd,
                sysinfo::DiskKind::SSD => DiskKind::Ssd,
                _ => DiskKind::Unknown,
            };
        }
    }
    DiskKind::Unknown
}

pub fn detect_hw(cache_root: &Path) -> HwInfo {
    HwInfo {
        logical_cpus: cpu_logical(),
        total_ram_mb: total_ram_mb(),
        disk_kind: disk_kind_for_path(cache_root),
    }
}
