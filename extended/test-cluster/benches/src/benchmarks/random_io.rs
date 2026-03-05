use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    report::section("2. Random I/O (4K + 64K, iodepth=32)");

    let runtime = config.fio_runtime_secs();
    let size = format!("{}M", config.seq_size_bytes() / (1024 * 1024));
    let mut results = Vec::new();

    for bs in ["4k", "64k"] {
        let mut local_iops = [0f64; 2]; // write, read
        let mut mfs_iops = [0f64; 2];
        let mut local_lat = [0f64; 2];
        let mut mfs_lat = [0f64; 2];

        for target in [Target::Local, Target::MooseFS] {
            let dir = util::target_dir(config, target, &format!("rand-{bs}"));
            util::cleanup(&dir);
            let dir_str = dir.to_str().unwrap();

            // Random write
            util::drop_caches();
            if let Some(json) = util::run_fio(&[
                &format!("--name=rand-write-{bs}"), &format!("--directory={dir_str}"),
                "--rw=randwrite", &format!("--bs={bs}"), &format!("--size={size}"),
                "--numjobs=1", "--ioengine=posixaio", "--direct=0", "--iodepth=32",
                &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
            ]) {
                let iops = util::fio_value(&json, "jobs.0.write.iops");
                let lat = util::fio_value(&json, "jobs.0.write.lat_ns.mean") / 1000.0;
                match target {
                    Target::Local => { local_iops[0] = iops; local_lat[0] = lat; }
                    Target::MooseFS => { mfs_iops[0] = iops; mfs_lat[0] = lat; }
                }
            }

            // Random read
            util::drop_caches();
            if let Some(json) = util::run_fio(&[
                &format!("--name=rand-read-{bs}"), &format!("--directory={dir_str}"),
                "--rw=randread", &format!("--bs={bs}"), &format!("--size={size}"),
                "--numjobs=1", "--ioengine=posixaio", "--direct=0", "--iodepth=32",
                &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
            ]) {
                let iops = util::fio_value(&json, "jobs.0.read.iops");
                let lat = util::fio_value(&json, "jobs.0.read.lat_ns.mean") / 1000.0;
                match target {
                    Target::Local => { local_iops[1] = iops; local_lat[1] = lat; }
                    Target::MooseFS => { mfs_iops[1] = iops; mfs_lat[1] = lat; }
                }
            }

            util::cleanup(&dir);
        }

        results.push(BenchResult::new(&format!("Rand Write IOPS ({bs})"), local_iops[0], mfs_iops[0], "IOPS", true));
        results.push(BenchResult::new(&format!("Rand Read IOPS ({bs})"), local_iops[1], mfs_iops[1], "IOPS", true));
        results.push(BenchResult::new(&format!("Rand Write Lat ({bs})"), local_lat[0], mfs_lat[0], "µs", false));
        results.push(BenchResult::new(&format!("Rand Read Lat ({bs})"), local_lat[1], mfs_lat[1], "µs", false));
    }

    results
}
