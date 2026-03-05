use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use super::util;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    report::section("6. Mixed Read/Write (70/30, 4K random, iodepth=16)");

    let runtime = config.fio_runtime_secs();
    let size = format!("{}M", config.seq_size_bytes() / (1024 * 1024));
    let mut local_vals = [0f64; 4]; // read_iops, write_iops, read_lat, write_lat
    let mut mfs_vals = [0f64; 4];

    for target in [Target::Local, Target::MooseFS] {
        let dir = util::target_dir(config, target, "mixed");
        util::cleanup(&dir);
        let dir_str = dir.to_str().unwrap();

        util::drop_caches();
        if let Some(json) = util::run_fio(&[
            "--name=mixed-rw", &format!("--directory={dir_str}"),
            "--rw=randrw", "--rwmixread=70", "--bs=4k",
            &format!("--size={size}"),
            "--numjobs=1", "--ioengine=posixaio", "--direct=0", "--iodepth=16",
            &format!("--runtime={runtime}"), "--time_based", "--group_reporting",
        ]) {
            let r_iops = util::fio_value(&json, "jobs.0.read.iops");
            let w_iops = util::fio_value(&json, "jobs.0.write.iops");
            let r_lat = util::fio_value(&json, "jobs.0.read.lat_ns.mean") / 1000.0;
            let w_lat = util::fio_value(&json, "jobs.0.write.lat_ns.mean") / 1000.0;

            match target {
                Target::Local => {
                    local_vals[0] = r_iops;
                    local_vals[1] = w_iops;
                    local_vals[2] = r_lat;
                    local_vals[3] = w_lat;
                }
                Target::MooseFS => {
                    mfs_vals[0] = r_iops;
                    mfs_vals[1] = w_iops;
                    mfs_vals[2] = r_lat;
                    mfs_vals[3] = w_lat;
                }
            }
        }

        util::cleanup(&dir);
    }

    vec![
        BenchResult::new("Mixed Read IOPS", local_vals[0], mfs_vals[0], "IOPS", true),
        BenchResult::new("Mixed Write IOPS", local_vals[1], mfs_vals[1], "IOPS", true),
        BenchResult::new("Mixed Read Lat", local_vals[2], mfs_vals[2], "µs", false),
        BenchResult::new("Mixed Write Lat", local_vals[3], mfs_vals[3], "µs", false),
    ]
}
