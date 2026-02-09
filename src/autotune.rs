use crate::hw::{DiskKind, HwInfo};

#[derive(Debug, Clone, Copy)]
pub struct MultiTune {
    pub workers: usize,
    pub duckdb_threads_per_job: usize,
}

pub fn auto_tune_single(hw: &HwInfo) -> usize {
    let cores = hw.logical_cpus.max(1);
    let ram = hw.total_ram_mb;

    if ram < 4096 {
        return 1;
    }
    if ram < 8192 {
        return 2.min(cores);
    }

    match hw.disk_kind {
        DiskKind::Ssd => (4.min(cores)).max(1),
        DiskKind::Hdd => (2.min(cores)).max(1),
        DiskKind::Unknown => (2.min(cores)).max(1),
    }
}

pub fn auto_tune_multi(hw: &HwInfo, files: usize) -> MultiTune {
    let cores = hw.logical_cpus.max(1);
    let ram = hw.total_ram_mb;

    let base_workers = match hw.disk_kind {
        DiskKind::Ssd => (cores / 2).max(2),
        DiskKind::Hdd => (cores / 3).max(1),
        DiskKind::Unknown => (cores / 3).max(1),
    };

    let ram_cap_workers = if ram < 4096 {
        1
    } else if ram < 8192 {
        2
    } else if ram < 16384 {
        4
    } else {
        8
    };

    let workers = base_workers.min(ram_cap_workers).min(files.max(1)).max(1);

    let duckdb_threads_per_job = if workers >= 6 {
        1
    } else if workers >= 3 {
        2.min(cores)
    } else {
        auto_tune_single(hw).max(1)
    };

    MultiTune {
        workers,
        duckdb_threads_per_job: duckdb_threads_per_job.max(1),
    }
}
