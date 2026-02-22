use serde::Deserialize;
use anyhow::Result;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Dataset {
    pub name: String,
    pub input_folder: String,
    pub output_file: String,
    pub dataset_type: String, // "single" hoặc "multi"

    pub select: Option<Vec<String>>,
    pub rename: Option<std::collections::HashMap<String, String>>,
    pub computed: Option<Vec<String>>,
    pub filters: Option<Vec<String>>,
    pub distinct: Option<bool>,
}

pub fn load_datasets(config_dir: &Path) -> Result<Vec<Dataset>> {
    let mut datasets = Vec::new();

    for entry in fs::read_dir(config_dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.extension().map(|e| e == "yaml").unwrap_or(false) {
            let content = fs::read_to_string(&path)?;
            let dataset: Dataset = serde_yaml::from_str(&content)?;
            datasets.push(dataset);
        }
    }

    Ok(datasets)
}