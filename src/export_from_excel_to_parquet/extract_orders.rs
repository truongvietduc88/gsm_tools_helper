use anyhow::{Result, anyhow};
use duckdb::Connection;
use num_cpus;

use crate::export_from_excel_to_parquet::types::Dataset;

/// Giữ nguyên signature cũ
pub fn process_dataset(dataset: &Dataset) -> Result<()> {

    // ===== DÙNG FILE LIST DO fs_scan CHUẨN BỊ =====
    let excel_files = &dataset.input_files;

    if excel_files.is_empty() {
        return Err(anyhow!(
            "Dataset {} không có file Excel",
            dataset.name
        ));
    }

    let conn = Connection::open_in_memory()?;

    // ===== CPU tuning =====
    let total_cores = num_cpus::get();
    let threads = ((total_cores as f64) * 0.7).ceil() as usize;

    conn.execute(&format!("PRAGMA threads={}", threads), [])?;
    conn.execute("PRAGMA memory_limit='70%'", [])?;

    let sql_body = build_sql(dataset, excel_files)?;

    let final_sql = format!(
        "COPY ({}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD)",
        sql_body,
        dataset.output_file
    );

    conn.execute(&final_sql, [])?;

    Ok(())
}
fn build_sql(dataset: &Dataset, files: &[std::path::PathBuf]) -> Result<String> {

    let select_clause = build_select_clause(dataset)?;
    let where_clause = build_where_clause(dataset)?;

    let is_multi = dataset.dataset_type.contains("_multi");
    let distinct = dataset.distinct.unwrap_or(false);

    if is_multi {

        let union_sql = files.iter()
            .map(|file| {
                format!(
                    "SELECT {} FROM read_excel_auto('{}') {}",
                    select_clause,
                    file.display(),
                    where_clause
                )
            })
            .collect::<Vec<_>>()
            .join(" UNION ALL ");

        if distinct {
            Ok(format!(
                "SELECT DISTINCT * FROM ({})",
                union_sql
            ))
        } else {
            Ok(union_sql)
        }

    } else {

        let file = files.first()
            .ok_or_else(|| anyhow!("Dataset SINGLE nhưng không có file"))?;

        if distinct {
            Ok(format!(
                "SELECT DISTINCT {} FROM read_excel_auto('{}') {}",
                select_clause,
                file.display(),
                where_clause
            ))
        } else {
            Ok(format!(
                "SELECT {} FROM read_excel_auto('{}') {}",
                select_clause,
                file.display(),
                where_clause
            ))
        }
    }
}
fn build_select_clause(dataset: &Dataset) -> Result<String> {

    let mut columns: Vec<String> = Vec::new();

    if let Some(select) = &dataset.select {
        columns.extend(select.clone());
    }

    if let Some(rename) = &dataset.rename {
        for (old, new) in rename {
            columns.push(format!("{} AS {}", old, new));
        }
    }

    if let Some(computed) = &dataset.computed {
        columns.extend(computed.clone());
    }

    if columns.is_empty() {
        Ok("*".to_string())
    } else {
        Ok(columns.join(", "))
    }
}

fn build_where_clause(dataset: &Dataset) -> Result<String> {

    if let Some(filters) = &dataset.filters {
        if !filters.is_empty() {
            return Ok(format!("WHERE {}", filters.join(" AND ")));
        }
    }

    Ok(String::new())
}