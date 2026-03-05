use crate::benchmarks;
use crate::db;
use crate::types::{BenchConfig, BenchResult};
use crate::Category;

/// Dispatch to the right benchmark module
pub fn run_category(cat: &Category, config: &BenchConfig) -> Vec<BenchResult> {
    match cat {
        Category::Sequential => benchmarks::sequential::run(config),
        Category::Random => benchmarks::random_io::run(config),
        Category::Metadata => benchmarks::metadata::run(config),
        Category::SmallFiles => benchmarks::small_files::run(config),
        Category::LargeFile => benchmarks::large_file::run(config),
        Category::Mixed => benchmarks::mixed::run(config),
        Category::Fsync => benchmarks::fsync::run(config),
        Category::DirScale => benchmarks::dir_scale::run(config),
        Category::Concurrent => benchmarks::concurrent::run(config),
        Category::Postgres => db::postgres::run(config),
        Category::Sqlite => db::sqlite::run(config),
        Category::Etcd => db::etcd::run(config),
        Category::All => unreachable!("expanded before dispatch"),
    }
}
