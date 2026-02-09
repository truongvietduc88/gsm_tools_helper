use crate::types::{FileStamp, RunMode, RunPlan};
use anyhow::{anyhow, Context, Result};
use blake3;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime},
};
use walkdir::WalkDir;

/// Bỏ file tạm Excel (~$...)
fn is_excel_temp_file(p: &Path) -> bool {
    p.file_name()
        .and_then(|s| s.to_str())
        .map(|s| s.starts_with("~$"))
        .unwrap_or(false)
}

/// Nhận xlsx/xlsb
fn is_excel_file(p: &Path) -> bool {
    let ext = p
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_lowercase();
    ext == "xlsx" || ext == "xlsb"
}

/// mtime unix (milliseconds)
fn mtime_unix_ms(meta: &fs::Metadata) -> Result<i64> {
    let m = meta.modified()?;
    let dur = m.duration_since(SystemTime::UNIX_EPOCH)?;
    Ok(dur.as_millis() as i64)
}

/// Hash nhanh: 64KB đầu + 64KB cuối
fn quick_hash_file(path: &Path, size: u64) -> Result<String> {
    const CHUNK: usize = 64 * 1024; // 64KB

    let mut f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = blake3::Hasher::new();

    // head
    let head_len = (size as usize).min(CHUNK);
    let mut buf = vec![0u8; head_len];
    let n = f.read(&mut buf).unwrap_or(0);
    hasher.update(&buf[..n]);

    // tail
    if (size as usize) > CHUNK {
        let tail_len = (size as usize).min(CHUNK);
        f.seek(SeekFrom::End(-(tail_len as i64)))
            .with_context(|| format!("seek tail {}", path.display()))?;
        let mut tail = vec![0u8; tail_len];
        let n2 = f.read(&mut tail).unwrap_or(0);
        hasher.update(&tail[..n2]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

/// Tạo FileStamp từ path
pub fn stat_file(p: &Path) -> Result<FileStamp> {
    let meta = fs::metadata(p).with_context(|| format!("metadata {}", p.display()))?;
    let size = meta.len();
    let mt_ms = mtime_unix_ms(&meta)?;

    let qh = quick_hash_file(p, size).unwrap_or_else(|_| "ERR".to_string());

    Ok(FileStamp {
        path: p.to_path_buf(),
        size,
        mtime_unix_ms: mt_ms,
        quick_hash: qh,
    })
}

/// file phải có "tuổi" >= stable_seconds.
fn is_stable(meta: &fs::Metadata, stable_seconds: i64) -> bool {
    let Ok(m) = meta.modified() else {
        return false;
    };
    let now = SystemTime::now();
    now.duration_since(m)
        .map(|d| d.as_secs() as i64 >= stable_seconds)
        .unwrap_or(false)
}

/// So size 2 lần để tránh đọc file đang copy
fn is_file_size_stable(path: &Path, interval_ms: u64) -> bool {
    let s1 = fs::metadata(path).map(|m| m.len()).ok();
    thread::sleep(Duration::from_millis(interval_ms));
    let s2 = fs::metadata(path).map(|m| m.len()).ok();
    s1.is_some() && s1 == s2
}

/// Single: lấy file mới nhất theo mtime trong folder *_single
pub fn pick_newest_excel(dir: &Path, stable_seconds: i64) -> Result<PathBuf> {
    let mut best: Option<(PathBuf, i64)> = None;

    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let p = entry.path();

        if !p.is_file() || is_excel_temp_file(&p) || !is_excel_file(&p) {
            continue;
        }

        let meta = entry.metadata()?;

        if stable_seconds <= 0 {
            if !is_file_size_stable(&p, 700) {
                continue;
            }
        } else if !is_stable(&meta, stable_seconds) {
            continue;
        }

        let mt_ms = mtime_unix_ms(&meta)?;
        match &best {
            None => best = Some((p, mt_ms)),
            Some((_, t)) if mt_ms > *t => best = Some((p, mt_ms)),
            _ => {}
        }
    }

    best.map(|(p, _)| p)
        .ok_or_else(|| anyhow!("No stable xlsx/xlsb found in {}", dir.display()))
}

/// Multi: lấy ALL excel trong folder *_multi (top-level)
pub fn list_all_excel(dir: &Path, stable_seconds: i64) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();

    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let p = entry.path();

        if !p.is_file() || is_excel_temp_file(&p) || !is_excel_file(&p) {
            continue;
        }

        let meta = entry.metadata()?;

        if stable_seconds <= 0 {
            if !is_file_size_stable(&p, 700) {
                continue;
            }
        } else if !is_stable(&meta, stable_seconds) {
            continue;
        }

        out.push(p);
    }

    out.sort();
    Ok(out)
}

fn parse_dataset_folder_name(name: &str) -> Option<(String, RunMode)> {
    let name = name.trim();

    if let Some(base) = name.strip_suffix("_single") {
        let base = base.trim();
        if !base.is_empty() {
            return Some((base.to_string(), RunMode::Single));
        }
    }
    if let Some(base) = name.strip_suffix("_multi") {
        let base = base.trim();
        if !base.is_empty() {
            return Some((base.to_string(), RunMode::Multi));
        }
    }
    None
}

/// List dataset base names
pub fn discover_datasets(input_root: &Path) -> Result<Vec<String>> {
    let mut set = BTreeSet::new();

    for entry in WalkDir::new(input_root).min_depth(1).max_depth(1) {
        let entry = entry?;
        if !entry.file_type().is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();
        if let Some((dataset, _mode)) = parse_dataset_folder_name(&name) {
            set.insert(dataset);
        }
    }

    Ok(set.into_iter().collect())
}

/// Dataset -> modes hiện có
pub fn discover_dataset_modes(input_root: &Path) -> Result<BTreeMap<String, BTreeSet<RunMode>>> {
    let mut map: BTreeMap<String, BTreeSet<RunMode>> = BTreeMap::new();

    for entry in WalkDir::new(input_root).min_depth(1).max_depth(1) {
        let entry = entry?;
        if !entry.file_type().is_dir() {
            continue;
        }

        let name = entry.file_name().to_string_lossy().to_string();
        if let Some((dataset, mode)) = parse_dataset_folder_name(&name) {
            map.entry(dataset).or_default().insert(mode);
        }
    }

    Ok(map)
}

pub fn dataset_dir(input_root: &Path, dataset: &str, mode: RunMode) -> PathBuf {
    let suffix = match mode {
        RunMode::Single => "_single",
        RunMode::Multi => "_multi",
    };
    input_root.join(format!("{}{}", dataset, suffix))
}

pub fn build_run_plan(
    input_root: &Path,
    dataset: &str,
    mode: RunMode,
    stable_seconds: i64,
) -> Result<RunPlan> {
    let dir = dataset_dir(input_root, dataset, mode);
    if !dir.exists() {
        return Err(anyhow!("Dataset folder not found: {}", dir.display()));
    }

    let files = match mode {
        RunMode::Single => vec![pick_newest_excel(&dir, stable_seconds)?],
        RunMode::Multi => list_all_excel(&dir, stable_seconds)?,
    };

    if files.is_empty() {
        return Err(anyhow!(
            "No input files found for dataset={} mode={:?}",
            dataset,
            mode
        ));
    }

    Ok(RunPlan {
        dataset: dataset.to_string(),
        mode,
        files,
    })
}
