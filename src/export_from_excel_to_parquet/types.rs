use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum RunMode {
    Single,
    Multi,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunPlan {
    pub dataset: String,
    pub mode: RunMode,
    pub files: Vec<PathBuf>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileStamp {
    pub path: PathBuf,
    pub size: u64,

    /// mtime theo milliseconds
    pub mtime_unix_ms: i64,

    /// hash nhanh để detect đổi nội dung
    pub quick_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheMeta {
    pub dataset: String,
    pub mode: RunMode,

    /// key = path string (lossy)
    pub stamps: BTreeMap<String, FileStamp>,
}
