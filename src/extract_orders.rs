// extract_orders.rs (PATCHED)

use anyhow::{anyhow, Context, Result};
use calamine::{open_workbook_auto, Data, Reader};
use duckdb::Connection;
use smallvec::SmallVec;
use std::fmt::Write as _;
use std::path::Path;

fn sanitize_col_name(raw: &str) -> String {
    raw.chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { '_' })
        .collect()
}

#[inline]
fn push_str_trim_if_needed(out: &mut String, s: &str) {
    if s.is_empty() {
        return;
    }
    let b = s.as_bytes();
    let first_ws = matches!(b.first(), Some(b' ' | b'\t' | b'\r' | b'\n'));
    let last_ws = matches!(b.last(), Some(b' ' | b'\t' | b'\r' | b'\n'));
    if first_ws || last_ws {
        out.push_str(s.trim());
    } else {
        out.push_str(s);
    }
}

#[inline]
fn cell_to_text_into(v: &Data, out: &mut String) {
    out.clear();
    match v {
        Data::Empty => {}
        Data::String(s) => push_str_trim_if_needed(out, s),
        Data::Bool(b) => {
            let _ = write!(out, "{}", b);
        }
        Data::Int(i) => {
            let _ = write!(out, "{}", i);
        }
        Data::Float(f) => {
            let _ = write!(out, "{}", f);
        }
        _ => {
            let s = v.to_string();
            push_str_trim_if_needed(out, &s);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColType {
    Unknown,
    Bool,
    Int,
    Double,
    Text,
}

fn type_name(t: ColType) -> &'static str {
    match t {
        ColType::Bool => "BOOLEAN",
        ColType::Int => "BIGINT",
        ColType::Double => "DOUBLE",
        ColType::Text | ColType::Unknown => "TEXT",
    }
}

#[inline]
fn float_is_integer(f: f64) -> bool {
    // Excel float đôi khi là 1.0 thay vì int
    f.is_finite() && (f.fract() == 0.0)
}

fn merge_type(cur: ColType, v: &Data) -> ColType {
    // Strategy:
    // - Nếu gặp String (native) => Text (không parse để tránh sai)
    // - Int + Float(frac) => Double
    // - Bool trộn với số => Text (tránh cast sai)
    // - Unknown -> type theo cell
    match v {
        Data::Empty => cur,

        Data::String(s) => {
            if s.trim().is_empty() {
                cur
            } else {
                ColType::Text
            }
        }

        Data::Bool(_) => match cur {
            ColType::Unknown | ColType::Bool => ColType::Bool,
            _ => ColType::Text,
        },

        Data::Int(_) => match cur {
            ColType::Unknown => ColType::Int,
            ColType::Int => ColType::Int,
            ColType::Double => ColType::Double,
            ColType::Bool => ColType::Text,
            ColType::Text => ColType::Text,
        },

        Data::Float(f) => {
            let as_int_like = float_is_integer(*f);
            match cur {
                ColType::Unknown => {
                    if as_int_like { ColType::Int } else { ColType::Double }
                }
                ColType::Int => {
                    if as_int_like { ColType::Int } else { ColType::Double }
                }
                ColType::Double => ColType::Double,
                ColType::Bool => ColType::Text,
                ColType::Text => ColType::Text,
            }
        }

        // Các kiểu khác (date/time/error/...) => TEXT an toàn
        _ => ColType::Text,
    }
}

fn infer_col_types<'a, I>(rows: I, ncols: usize, max_rows: usize) -> Vec<ColType>
where
    I: Iterator<Item = &'a [Data]>,
{
    let mut types = vec![ColType::Unknown; ncols];

    for (ri, row) in rows.take(max_rows).enumerate() {
        let _ = ri;
        for i in 0..ncols {
            if let Some(cell) = row.get(i) {
                let merged = merge_type(types[i], cell);
                types[i] = merged;
            }
        }
        // Nếu tất cả đã TEXT thì khỏi infer thêm
        if types.iter().all(|t| *t == ColType::Text) {
            break;
        }
    }

    // Unknown => TEXT
    for t in &mut types {
        if *t == ColType::Unknown {
            *t = ColType::Text;
        }
    }

    types
}

/// Convert 1 cell -> typed slot (no alloc for numeric/bool)
fn fill_typed_slot(
    cell: Option<&Data>,
    col_type: ColType,
    text_out: &mut String,
    out_i64: &mut Option<i64>,
    out_f64: &mut Option<f64>,
    out_bool: &mut Option<bool>,
) {
    // reset
    text_out.clear();
    *out_i64 = None;
    *out_f64 = None;
    *out_bool = None;

    let Some(v) = cell else {
        // missing cell => NULL/empty
        return;
    };

    match col_type {
        ColType::Text => {
            cell_to_text_into(v, text_out);
        }

        ColType::Bool => match v {
            Data::Empty => {}
            Data::Bool(b) => *out_bool = Some(*b),
            // nếu gặp thứ khác, fallback TEXT-ish thành NULL cho bool để tránh cast sai
            _ => {}
        },

        ColType::Int => match v {
            Data::Empty => {}
            Data::Int(i) => *out_i64 = Some(*i),
            Data::Float(f) => {
                if float_is_integer(*f) {
                    // clamp an toàn trong i64 range
                    if *f >= (i64::MIN as f64) && *f <= (i64::MAX as f64) {
                        *out_i64 = Some(*f as i64);
                    }
                }
            }
            _ => {}
        },

        ColType::Double => match v {
            Data::Empty => {}
            Data::Int(i) => *out_f64 = Some(*i as f64),
            Data::Float(f) => *out_f64 = Some(*f),
            _ => {}
        },

        ColType::Unknown => {
            // không xảy ra (đã normalize)
            cell_to_text_into(v, text_out);
        }
    }
}

/// MAX: Excel -> DuckDB -> Parquet
/// - Transaction + Appender
/// - typed columns (infer) => giảm format/alloc mạnh cho số
pub fn extract_one_excel_to_parquet(
    excel_path: &Path,
    parquet_out: &Path,
    duckdb_threads: usize,
) -> Result<()> {
    // 1) Open Excel
    let mut wb = open_workbook_auto(excel_path)
        .with_context(|| format!("open workbook {}", excel_path.display()))?;

    let sheet_name = wb
        .sheet_names()
        .get(0)
        .cloned()
        .ok_or_else(|| anyhow!("No sheet in {}", excel_path.display()))?;

    let range = wb
        .worksheet_range(&sheet_name)
        .with_context(|| format!("read sheet {}", sheet_name))?;

    let mut rows = range.rows();

    // 2) Header
    let header = rows.next().ok_or_else(|| anyhow!("Empty sheet"))?;

    let mut tmp = String::new();
    let mut cols: Vec<String> = Vec::with_capacity(header.len());
    for (i, c) in header.iter().enumerate() {
        cell_to_text_into(c, &mut tmp);
        let name = if tmp.is_empty() { format!("col_{i}") } else { tmp.clone() };
        cols.push(sanitize_col_name(&name));
    }

    // tránh trùng tên cột
    {
        use std::collections::HashMap;
        let mut seen: HashMap<String, usize> = HashMap::new();
        for c in cols.iter_mut() {
            let n = seen.entry(c.clone()).or_insert(0usize);
            if *n > 0 {
                *c = format!("{}_{}", c, n);
            }
            *n += 1;
        }
    }

    let ncols = cols.len();

    // 2.5) Infer types from first N rows (after header)
    const INFER_ROWS: usize = 200;
    let types = infer_col_types(rows.clone(), ncols, INFER_ROWS);

    // 3) DuckDB in-memory
    let conn = Connection::open_in_memory()?;

    let threads = duckdb_threads.clamp(1, 32);
    conn.execute(&format!("PRAGMA threads={}", threads), [])?;
    let _ = conn.execute("PRAGMA enable_progress_bar=false", []);
    println!("DuckDB threads = {}", threads);

    // 4) Create typed table
    let create_sql = format!(
        "CREATE TABLE t ({})",
        cols.iter()
            .zip(types.iter())
            .map(|(c, t)| format!("\"{}\" {}", c, type_name(*t)))
            .collect::<Vec<_>>()
            .join(", ")
    );
    conn.execute(&create_sql, [])?;

    // Transaction tăng tốc insert
    conn.execute("BEGIN TRANSACTION", [])?;

    // 5) Appender
    let mut app = conn.appender("t")?;

    // per-col reusable storage (stable refs per row)
    let mut text_vals: Vec<String> = (0..ncols).map(|_| String::new()).collect();
    let mut i64_vals: Vec<Option<i64>> = vec![None; ncols];
    let mut f64_vals: Vec<Option<f64>> = vec![None; ncols];
    let mut bool_vals: Vec<Option<bool>> = vec![None; ncols];

    let mut count: u64 = 0;

    // IMPORTANT: rows iterator đã clone() cho infer; giờ dùng "rows" thật để insert
    for row in rows {
        for i in 0..ncols {
            let cell = row.get(i);
            fill_typed_slot(
                cell,
                types[i],
                &mut text_vals[i],
                &mut i64_vals[i],
                &mut f64_vals[i],
                &mut bool_vals[i],
            );
        }

        let mut params: SmallVec<[&dyn duckdb::ToSql; 96]> = SmallVec::new();
        for i in 0..ncols {
            match types[i] {
                ColType::Text => params.push(&text_vals[i] as &dyn duckdb::ToSql),
                ColType::Int => params.push(&i64_vals[i] as &dyn duckdb::ToSql),
                ColType::Double => params.push(&f64_vals[i] as &dyn duckdb::ToSql),
                ColType::Bool => params.push(&bool_vals[i] as &dyn duckdb::ToSql),
                ColType::Unknown => params.push(&text_vals[i] as &dyn duckdb::ToSql),
            }
        }

        app.append_row(params.as_slice())?;
        count += 1;
    }

    app.flush()?;
    conn.execute("COMMIT", [])?;
    println!("Total rows inserted: {}", count);

    // 6) Export Parquet
    let out = parquet_out.to_string_lossy().replace('\\', "/");
    let copy_sql = format!("COPY t TO '{}' (FORMAT PARQUET, COMPRESSION SNAPPY)", out);
    conn.execute(&copy_sql, [])?;

    Ok(())
}
