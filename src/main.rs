mod app;
mod cache;
mod extract_orders;
mod fs_scan;
mod types;
mod hw;
mod autotune;
mod config;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use types::RunMode;

#[derive(Parser, Debug)]
#[command(author, version, about = "GSM Tools Helper (Rust)")]
struct Cli {
    #[arg(long)]
    input_root: Option<PathBuf>,

    #[arg(long)]
    cache_root: Option<PathBuf>,

    #[arg(long, default_value_t = 0)]
    stable_seconds: i64,

    #[arg(long)]
    workers: Option<usize>,

    #[arg(long)]
    duckdb_threads: Option<usize>,

    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    ListDatasets,
    Plan {
        #[arg(long)]
        dataset: String,
        #[arg(long, default_value = "single")]
        mode: String,
    },
    Extract {
        #[arg(long)]
        dataset: String,
        #[arg(long, default_value = "single")]
        mode: String,
    },
    RunAll,
}

fn parse_mode(s: &str) -> RunMode {
    match s.to_lowercase().as_str() {
        "multi" => RunMode::Multi,
        _ => RunMode::Single,
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // 🔥 LUÔN đi lên 3 cấp để về GSM_Mini_Tools
    let exe_path = std::env::current_exe()?;
    let base_dir = exe_path
        .parent()  // debug/
        .and_then(|p| p.parent()) // target/
        .and_then(|p| p.parent()) // gsm_tools_helper/
        .and_then(|p| p.parent()) // Sourcecode/
        .and_then(|p| p.parent()) // GSM_Mini_Tools/
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    let input_root = cli
        .input_root
        .unwrap_or_else(|| base_dir.join("input_File"));

    let cache_root = cli
        .cache_root
        .unwrap_or_else(|| base_dir.join("cache"));

    println!("INPUT ROOT = {:?}", input_root);
    println!("CACHE ROOT = {:?}", cache_root);

    std::fs::create_dir_all(&input_root)?;
    std::fs::create_dir_all(&cache_root)?;

    let hwinfo = hw::detect_hw(&cache_root);
    println!(
        "HW DETECTED: cores(logical)={}, ram={}MB, disk={:?}",
        hwinfo.logical_cpus, hwinfo.total_ram_mb, hwinfo.disk_kind
    );

    let cfg = app::AppConfig {
        input_root,
        cache_root,
        stable_seconds: cli.stable_seconds,
        workers: cli.workers,
        duckdb_threads: cli.duckdb_threads,
        hw: hwinfo,
    };

    app::auto_cleanup_cache(&cfg)?;

    match cli.cmd {
        Commands::ListDatasets => app::list_datasets(&cfg)?,
        Commands::Plan { dataset, mode } => {
            app::plan(&cfg, &dataset, parse_mode(&mode))?
        }
        Commands::Extract { dataset, mode } => {
            app::extract(&cfg, &dataset, parse_mode(&mode))?
        }
        Commands::RunAll => app::run_all(&cfg)?,
    }

    Ok(())
}
