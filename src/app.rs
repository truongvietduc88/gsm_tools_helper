use crate::extract_orders::extract_one_excel_to_parquet;
use crate::hw::HwInfo;
use crate::{autotune, cache, fs_scan, types::RunMode};

use anyhow::{anyhow, Result};
use rayon::prelude::*;
use regex::Regex;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use time::{OffsetDateTime, UtcOffset};

pub struct AppConfig {
    pub input_root: PathBuf,
    pub cache_root: PathBuf,
    pub stable_seconds: i64,

    pub workers: Option<usize>,
    pub duckdb_threads: Option<usize>,

    pub hw: HwInfo,
}

/* =========================
   Helpers
   ========================= */

fn size_mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn should_heavy_first(sorted_desc: &[crate::types::FileStamp]) -> bool {
    if sorted_desc.len() < 2 {
        return false;
    }
    let max_mb = size_mb(sorted_desc[0].size);
    let second_mb = size_mb(sorted_desc[1].size);

    max_mb >= 30.0 || (second_mb > 0.0 && max_mb >= 1.5 * second_mb)
}

pub fn list_datasets(cfg: &AppConfig) -> Result<()> {
    let ds = fs_scan::discover_datasets(&cfg.input_root)?;
    println!("Datasets discovered:");
    for d in ds {
        println!("  - {}", d);
    }
    Ok(())
}

/// ✅ FIX: plan chỉ “xem”, KHÔNG ghi cache nữa.
/// Nếu ghi cache ở plan -> extract sẽ thấy “no changes” và skip.
pub fn plan(cfg: &AppConfig, dataset: &str, mode: RunMode) -> Result<()> {
    let plan = fs_scan::build_run_plan(&cfg.input_root, dataset, mode, cfg.stable_seconds)?;

    println!("=== RUN PLAN ===");
    println!("Dataset : {}", plan.dataset);
    println!("Mode    : {:?}", plan.mode);
    println!("Files   :");
    for f in &plan.files {
        println!("  - {}", f.display());
    }

    let mut stamps = Vec::with_capacity(plan.files.len());
    for f in &plan.files {
        stamps.push(fs_scan::stat_file(f)?);
    }

    let prev = cache::load_cache(&cfg.cache_root, dataset, mode);
    let (_next, changed) = cache::diff_files(prev, dataset, mode, &stamps);

    println!("\n=== CACHE DIFF ===");
    if changed.is_empty() {
        println!("No changes detected => reuse cached parquet (if exists).");
    } else {
        println!("Changed/New files => need rebuild parquet:");
        for st in &changed {
            println!(
                "  - {} (size={}, mtime_ms={})",
                st.path.display(),
                st.size,
                st.mtime_unix_ms
            );
        }
    }

    Ok(())
}

pub fn extract(cfg: &AppConfig, dataset: &str, mode: RunMode) -> Result<()> {
    let plan = fs_scan::build_run_plan(&cfg.input_root, dataset, mode, cfg.stable_seconds)?;

    let mut stamps = Vec::with_capacity(plan.files.len());
    for f in &plan.files {
        stamps.push(fs_scan::stat_file(f)?);
    }

    let prev = cache::load_cache(&cfg.cache_root, dataset, mode);

    match mode {
        RunMode::Single => extract_single(cfg, dataset, &plan.files, &stamps, prev),
        RunMode::Multi => extract_multi(cfg, dataset, &stamps, prev),
    }
}

/* =========================
   NEW: RUN ALL (auto sync)
   ========================= */

/// Chạy hết dataset/mode hiện có trong input_root:
/// - dataset nào có *_single -> extract single
/// - dataset nào có *_multi  -> extract multi
pub fn run_all(cfg: &AppConfig) -> Result<()> {
    let alive = fs_scan::discover_dataset_modes(&cfg.input_root)?;

    println!("=== RUN ALL ===");
    for (dataset, modes) in alive {
        if modes.contains(&RunMode::Single) {
            println!("\n--- dataset={} mode=single ---", dataset);
            let _ = extract(cfg, &dataset, RunMode::Single)?;
        }
        if modes.contains(&RunMode::Multi) {
            println!("\n--- dataset={} mode=multi ---", dataset);
            let _ = extract(cfg, &dataset, RunMode::Multi)?;
        }
    }
    Ok(())
}

/* =========================
   Single
   ========================= */

fn extract_single(
    cfg: &AppConfig,
    dataset: &str,
    plan_files: &[PathBuf],
    stamps: &[crate::types::FileStamp],
    prev: Option<crate::types::CacheMeta>,
) -> Result<()> {
    let (next, changed) = cache::diff_files(prev, dataset, RunMode::Single, stamps);

    let single_dir = cfg.cache_root.join(dataset).join("single");
    std::fs::create_dir_all(&single_dir)?;

    let current_txt = single_dir.join("current_parquet.txt");
    let old_parquet = read_optional_trimmed(&current_txt).unwrap_or_default();

    let old_parquet_path = if old_parquet.is_empty() {
        None
    } else {
        Some(single_dir.join(&old_parquet))
    };

    let parquet_missing = old_parquet_path
        .as_ref()
        .map(|p| !p.exists())
        .unwrap_or(true);

    if changed.is_empty() && !parquet_missing {
        println!("No changes => reuse cached parquet.");
        // vẫn save cache để lần sau đúng
        cache::save_cache(&cfg.cache_root, &next)?;
        return Ok(());
    }

    println!(
        "Extracting single (changed_files={}, parquet_missing={})",
        changed.len(),
        parquet_missing
    );

    let st = fs_scan::stat_file(&plan_files[0])?;
    let new_name = build_parquet_name(&plan_files[0], st.mtime_unix_ms);
    let new_parquet = single_dir.join(&new_name);

    let duckdb_threads = cfg
        .duckdb_threads
        .unwrap_or_else(|| autotune::auto_tune_single(&cfg.hw));

    extract_one_excel_to_parquet(&plan_files[0], &new_parquet, duckdb_threads)?;
    println!("Wrote: {}", new_parquet.display());

    if let Some(oldp) = old_parquet_path {
        if oldp.exists() && oldp != new_parquet {
            let _ = std::fs::remove_file(&oldp);
            println!("Removed old parquet: {}", oldp.display());
        }
    }

    std::fs::write(&current_txt, format!("{new_name}\n"))?;
    cache::save_cache(&cfg.cache_root, &next)?;
    Ok(())
}

/* =========================
   Multi
   ========================= */

fn extract_multi(
    cfg: &AppConfig,
    dataset: &str,
    stamps: &[crate::types::FileStamp],
    prev: Option<crate::types::CacheMeta>,
) -> Result<()> {
    let multi_dir = cfg.cache_root.join(dataset).join("multi");
    let daily_dir = multi_dir.join("daily");
    std::fs::create_dir_all(&daily_dir)?;

    let map_path = multi_dir.join("parquet_map.tsv");

    // cleanup: input deleted => remove parquet
    if let Some(prev_meta) = prev.clone() {
        let current: HashSet<String> = stamps
            .iter()
            .map(|s| s.path.to_string_lossy().to_string())
            .collect();

        let mut map = load_tsv_map(&map_path);

        for (_k, old) in prev_meta.stamps {
            let old_path = old.path.to_string_lossy().to_string();
            if !current.contains(&old_path) {
                if let Some(old_parquet_name) = map.remove(&old_path) {
                    let out_parquet = daily_dir.join(&old_parquet_name);
                    if out_parquet.exists() {
                        let _ = std::fs::remove_file(&out_parquet);
                        println!("Removed parquet (input deleted): {}", out_parquet.display());
                    }
                }
            }
        }

        save_tsv_map(&map_path, &map)?;
    }

    // diff
    let (next, mut changed) = cache::diff_files(prev, dataset, RunMode::Multi, stamps);

    // ✅ FIX quan trọng:
    // Nếu cache báo "no changes" nhưng output thiếu (map/parquet) => ép chạy lại.
    if changed.is_empty() {
        if !multi_outputs_ok(&map_path, &daily_dir, stamps) {
            println!("Cache says no changes, but parquet/map missing => force rebuild.");
            changed = stamps.to_vec();
        } else {
            println!("No changes => reuse cached parquet.");
            cache::save_cache(&cfg.cache_root, &next)?;
            return Ok(());
        }
    }

    // sort by size desc
    changed.sort_by(|a, b| b.size.cmp(&a.size));

    println!("Extracting changed files: {}", changed.len());
    for (i, st) in changed.iter().enumerate() {
        println!("  [{}] {} ({} MB)", i, st.path.display(), size_mb(st.size));
    }

    let heavy_first = should_heavy_first(&changed);

    let tuned = autotune::auto_tune_multi(&cfg.hw, changed.len());
    let workers = cfg
        .workers
        .unwrap_or(tuned.workers)
        .max(1)
        .min(changed.len());

    let duckdb_threads_parallel = cfg
        .duckdb_threads
        .unwrap_or(tuned.duckdb_threads_per_job)
        .max(1);

    println!(
        "AUTO-TUNE: disk={:?}, cores={}, ram={}MB, files={}, workers={}, duckdb_threads/job={}, heavy_first={} (override workers={:?}, duckdb_override={:?})",
        cfg.hw.disk_kind,
        cfg.hw.logical_cpus,
        cfg.hw.total_ram_mb,
        changed.len(),
        workers,
        duckdb_threads_parallel,
        heavy_first,
        cfg.workers,
        cfg.duckdb_threads
    );

    let mut map = load_tsv_map(&map_path);
    let mut ok_count: usize = 0;
    let mut fail_list: Vec<(String, String)> = Vec::new();

    // Phase A: heavy alone
    let mut idx_start = 0usize;
    if heavy_first {
        let st = &changed[0];
        let parquet_name = build_parquet_name(&st.path, st.mtime_unix_ms);
        let out_parquet = daily_dir.join(&parquet_name);

        let duckdb_threads_heavy = cfg
            .duckdb_threads
            .unwrap_or_else(|| autotune::auto_tune_single(&cfg.hw))
            .max(1);

        println!(
            "Phase A (heavy alone): {} ({} MB), duckdb_threads={}",
            st.path.display(),
            size_mb(st.size),
            duckdb_threads_heavy
        );

        match extract_one_excel_to_parquet(&st.path, &out_parquet, duckdb_threads_heavy) {
            Ok(_) => {
                println!("Wrote: {}", out_parquet.display());
                map.insert(st.path.to_string_lossy().to_string(), parquet_name);
                ok_count += 1;
            }
            Err(e) => fail_list.push((st.path.to_string_lossy().to_string(), format!("{:#}", e))),
        }

        idx_start = 1;
    }

    let remaining = &changed[idx_start..];
    if remaining.is_empty() {
        save_tsv_map(&map_path, &map)?;
        cache::save_cache(&cfg.cache_root, &next)?;
        if fail_list.is_empty() {
            println!("DONE: success={}", ok_count);
            return Ok(());
        }
        return Err(anyhow!(
            "Multi extract finished with {} failed file(s).",
            fail_list.len()
        ));
    }

    // Phase B parallel
    let workers_b = workers.min(remaining.len()).max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers_b)
        .build()
        .map_err(|e| anyhow!("rayon threadpool build failed: {}", e))?;

    #[derive(Debug)]
    struct OkItem {
        input_path: String,
        parquet_name: String,
        out_display: String,
        day: String,
    }

    let daily_dir_cloned = daily_dir.clone();
    let results: Vec<(String, std::result::Result<OkItem, anyhow::Error>)> = pool.install(|| {
        remaining
            .par_iter()
            .map(|st| {
                let key = st.path.to_string_lossy().to_string();

                let r: anyhow::Result<OkItem> = (|| {
                    let day = yyyy_mm_dd_from_orders_admin_filename(&st.path)
                        .unwrap_or_else(|| "unknown-date".to_string());

                    let parquet_name = build_parquet_name(&st.path, st.mtime_unix_ms);
                    let out_parquet = daily_dir_cloned.join(&parquet_name);

                    extract_one_excel_to_parquet(&st.path, &out_parquet, duckdb_threads_parallel)?;

                    Ok(OkItem {
                        input_path: key.clone(),
                        parquet_name,
                        out_display: out_parquet.display().to_string(),
                        day,
                    })
                })();

                (key, r)
            })
            .collect()
    });

    for (input_path, r) in results {
        match r {
            Ok(ok) => {
                println!("Wrote: {} (day={})", ok.out_display, ok.day);
                map.insert(ok.input_path, ok.parquet_name);
                ok_count += 1;
            }
            Err(e) => fail_list.push((input_path, format!("{:#}", e))),
        }
    }

    save_tsv_map(&map_path, &map)?;
    cache::save_cache(&cfg.cache_root, &next)?;

    if fail_list.is_empty() {
        println!("DONE: success={}", ok_count);
        Ok(())
    } else {
        println!(
            "DONE WITH ERRORS: success={}, failed={}",
            ok_count,
            fail_list.len()
        );
        for (p, err) in &fail_list {
            println!("  - {}\n    {}", p, err);
        }
        Err(anyhow!(
            "Multi extract finished with {} failed file(s).",
            fail_list.len()
        ))
    }
}

/// ✅ output check cho Multi:
/// - parquet_map.tsv phải tồn tại
/// - mỗi input file phải có entry và parquet file tương ứng tồn tại
fn multi_outputs_ok(map_path: &Path, daily_dir: &Path, stamps: &[crate::types::FileStamp]) -> bool {
    if !map_path.exists() {
        return false;
    }
    if !daily_dir.exists() {
        return false;
    }

    let map = load_tsv_map(map_path);
    if map.is_empty() {
        return false;
    }

    for st in stamps {
        let key = st.path.to_string_lossy().to_string();
        let Some(parquet_name) = map.get(&key) else {
            return false;
        };
        let p = daily_dir.join(parquet_name);
        if !p.exists() {
            return false;
        }
    }
    true
}

fn yyyy_mm_dd_from_orders_admin_filename(p: &Path) -> Option<String> {
    let name = p.file_name()?.to_str()?;
    let re = Regex::new(r"(?i)_(\d{2})_(\d{2})_(\d{4})\.(xlsx|xlsb)$").ok()?;
    let caps = re.captures(name)?;

    let dd = &caps[1];
    let mm = &caps[2];
    let yyyy = &caps[3];

    Some(format!("{yyyy}-{mm}-{dd}"))
}

pub fn auto_cleanup_cache(cfg: &AppConfig) -> Result<()> {
    let alive = fs_scan::discover_dataset_modes(&cfg.input_root)?;

    if !cfg.cache_root.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(&cfg.cache_root)? {
        let entry = entry?;
        let ds_dir = entry.path();
        if !ds_dir.is_dir() {
            continue;
        }

        let dataset = entry.file_name().to_string_lossy().to_string();

        match alive.get(&dataset) {
            None => {
                println!("[AUTO-CLEAN] Removed orphan dataset cache: {}", ds_dir.display());
                let _ = std::fs::remove_dir_all(&ds_dir);
            }
            Some(modes) => {
                let single_dir = ds_dir.join("single");
                if !modes.contains(&RunMode::Single) && single_dir.exists() {
                    println!("[AUTO-CLEAN] Removed orphan single cache: {}", single_dir.display());
                    let _ = std::fs::remove_dir_all(&single_dir);
                }

                let multi_dir = ds_dir.join("multi");
                if !modes.contains(&RunMode::Multi) && multi_dir.exists() {
                    println!("[AUTO-CLEAN] Removed orphan multi cache: {}", multi_dir.display());
                    let _ = std::fs::remove_dir_all(&multi_dir);
                }
            }
        }
    }

    Ok(())
}

/* =========================
   Helpers: parquet naming + map storage
   ========================= */

fn sanitize_stem(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn now_vn() -> OffsetDateTime {
    let vn = UtcOffset::from_hms(7, 0, 0).unwrap();
    OffsetDateTime::now_utc().to_offset(vn)
}

fn now_ddmmyyyy_hhmmss_vn() -> String {
    let now = now_vn();
    let d = now.day();
    let m: u8 = now.month() as u8;
    let y = now.year();
    let hh = now.hour();
    let mm = now.minute();
    let ss = now.second();

    format!("{:02}{:02}{:04}_{:02}{:02}{:02}", d, m, y, hh, mm, ss)
}

fn build_parquet_name(excel_path: &Path, mtime_unix_ms: i64) -> String {
    let stem = excel_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("file");

    let stem = sanitize_stem(stem);
    let ts = now_ddmmyyyy_hhmmss_vn();

    format!("{}_{}_{}.parquet", stem, ts, mtime_unix_ms)
}

fn read_optional_trimmed(p: &Path) -> Option<String> {
    let s = std::fs::read_to_string(p).ok()?;
    let t = s.trim().to_string();
    if t.is_empty() { None } else { Some(t) }
}

fn load_tsv_map(path: &Path) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    let Ok(s) = std::fs::read_to_string(path) else {
        return map;
    };

    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once('\t') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if !k.is_empty() && !v.is_empty() {
                map.insert(k, v);
            }
        }
    }

    map
}

fn save_tsv_map(path: &Path, map: &BTreeMap<String, String>) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let mut out = String::new();
    for (k, v) in map {
        out.push_str(k);
        out.push('\t');
        out.push_str(v);
        out.push('\n');
    }
    std::fs::write(path, out)?;
    Ok(())
}
