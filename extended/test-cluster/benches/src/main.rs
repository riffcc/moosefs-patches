mod benchmarks;
mod db;
mod report;
mod runner;
mod types;

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "mfs-bench", about = "MooseFS Benchmark Suite")]
struct Args {
    /// Local filesystem directory for baseline
    #[arg(long, default_value = "/tmp/bench-local")]
    local_dir: PathBuf,

    /// MooseFS mount directory
    #[arg(long, default_value = "/mnt/mfs/bench-mfs")]
    mfs_dir: PathBuf,

    /// Quick mode (smaller datasets, shorter runs)
    #[arg(long)]
    quick: bool,

    /// Which benchmark category to run (default: all)
    #[arg(value_enum, default_value = "all")]
    category: Category,

    /// Output CSV path
    #[arg(long, default_value = "/tmp/bench-results/results.csv")]
    csv: PathBuf,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
enum Category {
    All,
    Sequential,
    Random,
    Metadata,
    SmallFiles,
    LargeFile,
    Mixed,
    Fsync,
    DirScale,
    Concurrent,
    Postgres,
    Sqlite,
    Etcd,
}

fn main() {
    let args = Args::parse();

    let config = types::BenchConfig {
        local_dir: args.local_dir,
        mfs_dir: args.mfs_dir,
        quick: args.quick,
        csv_path: args.csv,
    };

    // Ensure directories exist
    std::fs::create_dir_all(&config.local_dir).ok();
    std::fs::create_dir_all(&config.mfs_dir).ok();
    if let Some(parent) = config.csv_path.parent() {
        std::fs::create_dir_all(parent).ok();
    }

    report::print_banner(&config);

    let mut all_results = Vec::new();

    let categories: Vec<Category> = if args.category == Category::All {
        vec![
            Category::Sequential,
            Category::Random,
            Category::Metadata,
            Category::SmallFiles,
            Category::LargeFile,
            Category::Mixed,
            Category::Fsync,
            Category::DirScale,
            Category::Concurrent,
            Category::Postgres,
            Category::Sqlite,
            Category::Etcd,
        ]
    } else {
        vec![args.category]
    };

    for cat in &categories {
        let results = runner::run_category(cat, &config);
        all_results.extend(results);
    }

    report::print_summary(&all_results);
    report::write_csv(&config.csv_path, &all_results);
}
