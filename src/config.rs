use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct YamlConfig {
    pub transform: Option<TransformConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    pub select: Option<Vec<String>>,
    pub rename: Option<HashMap<String, String>>,
    pub filters: Option<Vec<String>>,
    pub computed: Option<Vec<ComputedColumn>>,
    pub distinct: Option<bool>,
}

#[derive(Debug, Deserialize)]
pub struct ComputedColumn {
    pub name: String,
    pub expr: String,
}

pub fn load_yaml(path: &Path) -> Option<YamlConfig> {
    if !path.exists() {
        return None;
    }
    let file = File::open(path).ok()?;
    serde_yaml::from_reader(file).ok()
}
