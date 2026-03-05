use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let size = config.seq_size_bytes();
    let runtime = config.fio_runtime_secs();
    let size_str = format!("{}M", size / (1024 * 1024));
    report::section(&format!("1. Sequential I/O (bs=1M, size={size_str})"));

    let mut results = Vec::new();
    let mut local_vals = [0f64; 4]; // wr_bw, rd_bw, wr_lat, rd_lat
    let mut mfs_vals = [0f64; 4];

    for target in [Target::Local, Target::MooseFS] {
        let dir = util::target_dir(config, target, "seq");
        util::cleanup(&dir);

        // Sequential write
        util::drop_caches();
        let dir_str = dir.to_str().unwrap();
        if let Some(json) = util::run_fio(&[
            "--name=seq-write", &format!("--directory={dir_str}"),
            "--rw=write", "--bs=1M", &format!("--size={size_str}"),
            "--numjobs=1", "--ioengine=posixaio", "--direct=0",
            &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
        ]) {
            let bw = util::fio_value(&json, "jobs.0.write.bw_bytes") / (1024.0 * 1024.0);
            let lat = util::fio_value(&json, "jobs.0.write.lat_ns.mean") / 1000.0;
            match target {
                Target::Local => { local_vals[0] = bw; local_vals[2] = lat; }
                Target::MooseFS => { mfs_vals[0] = bw; mfs_vals[2] = lat; }
            }
        }

        // Sequential read
        util::drop_caches();
        if let Some(json) = util::run_fio(&[
            "--name=seq-read", &format!("--directory={dir_str}"),
            "--rw=read", "--bs=1M", &format!("--size={size_str}"),
            "--numjobs=1", "--ioengine=posixaio", "--direct=0",
            &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
        ]) {
            let bw = util::fio_value(&json, "jobs.0.read.bw_bytes") / (1024.0 * 1024.0);
            let lat = util::fio_value(&json, "jobs.0.read.lat_ns.mean") / 1000.0;
            match target {
                Target::Local => { local_vals[1] = bw; local_vals[3] = lat; }
                Target::MooseFS => { mfs_vals[1] = bw; mfs_vals[3] = lat; }
            }
        }

        util::cleanup(&dir);
    }

    results.push(BenchResult::new("Seq Write Throughput", local_vals[0], mfs_vals[0], "MB/s", true));
    results.push(BenchResult::new("Seq Read Throughput", local_vals[1], mfs_vals[1], "MB/s", true));
    results.push(BenchResult::new("Seq Write Avg Latency", local_vals[2], mfs_vals[2], "µs", false));
    results.push(BenchResult::new("Seq Read Avg Latency", local_vals[3], mfs_vals[3], "µs", false));

    results
}
