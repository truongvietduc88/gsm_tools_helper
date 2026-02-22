use crate::types::{CacheMeta, FileStamp, RunMode};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::path::Path;

/// cache file path: <cache_root>/<dataset>/<mode>_cache.json
fn cache_path(cache_root: &Path, dataset: &str, mode: RunMode) -> std::path::PathBuf {
    let file = match mode {
        RunMode::Single => "single_cache.json",
        RunMode::Multi => "multi_cache.json",
    };
    cache_root.join(dataset).join(file)
}

pub fn load_cache(cache_root: &Path, dataset: &str, mode: RunMode) -> Option<CacheMeta> {
    let p = cache_path(cache_root, dataset, mode);
    let s = std::fs::read_to_string(&p).ok()?;
    serde_json::from_str(&s).ok()
}

pub fn save_cache(cache_root: &Path, meta: &CacheMeta) -> Result<()> {
    let p = cache_path(cache_root, &meta.dataset, meta.mode);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let s = serde_json::to_string_pretty(meta)?;
    std::fs::write(&p, s).with_context(|| format!("write {}", p.display()))?;
    Ok(())
}

/// Diff danh sách file hiện tại với cache cũ.
/// Trả (next_cache, changed_files)
///
/// detect thay đổi dựa trên (size + mtime_ms + quick_hash).
pub fn diff_files(
    prev: Option<CacheMeta>,
    dataset: &str,
    mode: RunMode,
    current: &[FileStamp],
) -> (CacheMeta, Vec<FileStamp>) {
    let mut next_map: BTreeMap<String, FileStamp> = BTreeMap::new();
    let mut changed: Vec<FileStamp> = Vec::new();

    let prev_map: BTreeMap<String, FileStamp> =
        prev.map(|m| m.stamps).unwrap_or_else(BTreeMap::new);

    for st in current {
        let key = st.path.to_string_lossy().to_string();
        next_map.insert(key.clone(), st.clone());

        match prev_map.get(&key) {
            None => changed.push(st.clone()),
            Some(old) => {
                let same = old.size == st.size
                    && old.mtime_unix_ms == st.mtime_unix_ms
                    && old.quick_hash == st.quick_hash;

                if !same {
                    changed.push(st.clone());
                }
            }
        }
    }

    let next = CacheMeta {
        dataset: dataset.to_string(),
        mode,
        stamps: next_map,
    };

    (next, changed)
}
