use anyhow::Result;
use rayon::prelude::*;
use std::path::Path;

use crate::export_from_excel_to_parquet::config::load_datasets;
use crate::export_from_excel_to_parquet::extract_orders::process_dataset;

pub fn run() -> Result<()> {
    run_all()
}

pub fn run_all() -> Result<()> {
    let config_dir = Path::new("src/export_from_excel_to_parquet/configYAML");

    let datasets = load_datasets(config_dir)?;

    // Chạy tối đa 4 dataset song song
    datasets.par_iter().take(6).for_each(|ds| {
        if let Err(e) = process_dataset(ds) {
            eprintln!("Dataset {} failed: {:?}", ds.name, e);
        }
    });

    Ok(())
}