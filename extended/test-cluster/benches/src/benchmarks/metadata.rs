use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::time::Instant;

struct MetaOps {
    create: f64,
    stat: f64,
    chmod: f64,
    rename: f64,
    unlink: f64,
    mkdir: f64,
    rmdir: f64,
}

fn run_meta_ops(config: &BenchConfig, target: Target) -> MetaOps {
    let count = config.small_file_count();
    let dir = util::target_dir(config, target, "meta");
    util::cleanup(&dir);

    // create
    let start = Instant::now();
    for i in 0..count {
        fs::File::create(dir.join(format!("f{i}"))).ok();
    }
    let create = count as f64 / start.elapsed().as_secs_f64();

    // stat
    util::drop_caches();
    let start = Instant::now();
    for i in 0..count {
        fs::metadata(dir.join(format!("f{i}"))).ok();
    }
    let stat = count as f64 / start.elapsed().as_secs_f64();

    // chmod
    let start = Instant::now();
    for i in 0..count {
        fs::set_permissions(dir.join(format!("f{i}")), fs::Permissions::from_mode(0o644)).ok();
    }
    let chmod = count as f64 / start.elapsed().as_secs_f64();

    // rename
    let start = Instant::now();
    for i in 0..count {
        fs::rename(dir.join(format!("f{i}")), dir.join(format!("r{i}"))).ok();
    }
    let rename = count as f64 / start.elapsed().as_secs_f64();

    // unlink
    let start = Instant::now();
    for i in 0..count {
        fs::remove_file(dir.join(format!("r{i}"))).ok();
    }
    let unlink = count as f64 / start.elapsed().as_secs_f64();

    // mkdir
    let start = Instant::now();
    for i in 0..count {
        fs::create_dir(dir.join(format!("d{i}"))).ok();
    }
    let mkdir = count as f64 / start.elapsed().as_secs_f64();

    // rmdir
    let start = Instant::now();
    for i in 0..count {
        fs::remove_dir(dir.join(format!("d{i}"))).ok();
    }
    let rmdir = count as f64 / start.elapsed().as_secs_f64();

    util::cleanup(&dir);

    MetaOps { create, stat, chmod, rename, unlink, mkdir, rmdir }
}

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let count = config.small_file_count();
    report::section(&format!("3. Metadata Operations ({count} files)"));

    let local = run_meta_ops(config, Target::Local);
    let mfs = run_meta_ops(config, Target::MooseFS);

    vec![
        BenchResult::new("Create ops/s", local.create, mfs.create, "ops/s", true),
        BenchResult::new("Stat ops/s", local.stat, mfs.stat, "ops/s", true),
        BenchResult::new("Chmod ops/s", local.chmod, mfs.chmod, "ops/s", true),
        BenchResult::new("Rename ops/s", local.rename, mfs.rename, "ops/s", true),
        BenchResult::new("Unlink ops/s", local.unlink, mfs.unlink, "ops/s", true),
        BenchResult::new("Mkdir ops/s", local.mkdir, mfs.mkdir, "ops/s", true),
        BenchResult::new("Rmdir ops/s", local.rmdir, mfs.rmdir, "ops/s", true),
    ]
}
