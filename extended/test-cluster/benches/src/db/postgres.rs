use crate::report;
use crate::types::{BenchConfig, BenchResult};

use std::process::Command;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let scale = config.pgbench_scale();
    let time = config.pgbench_time_secs();
    report::section(&format!("10. PostgreSQL pgbench (scale={scale}, time={time}s)"));

    let mut results = Vec::new();

    // We run pgbench against two PG instances:
    //   - One with PGDATA on local disk
    //   - One with PGDATA on MooseFS
    // The test cluster should expose these as different ports or hosts.
    // Fallback: use environment variables.
    let local_connstr = std::env::var("BENCH_PG_LOCAL")
        .unwrap_or_else(|_| "host=localhost port=5432 dbname=bench user=bench".to_string());
    let mfs_connstr = std::env::var("BENCH_PG_MFS")
        .unwrap_or_else(|_| "host=localhost port=5433 dbname=bench user=bench".to_string());

    let local_tps = run_pgbench(&local_connstr, scale, time);
    let mfs_tps = run_pgbench(&mfs_connstr, scale, time);

    results.push(BenchResult::new("pgbench TPS", local_tps, mfs_tps, "TPS", true));

    // Also test select-only (read-heavy) workload
    let local_select = run_pgbench_select(&local_connstr, time);
    let mfs_select = run_pgbench_select(&mfs_connstr, time);

    results.push(BenchResult::new("pgbench Select-Only TPS", local_select, mfs_select, "TPS", true));

    results
}

fn run_pgbench(connstr: &str, scale: u32, time: u32) -> f64 {
    // Initialize
    let init = Command::new("pgbench")
        .args(["-i", &format!("-s{scale}"), "-d"])
        .env("PGCONNSTRING", connstr)
        .arg(connstr_to_dbname(connstr))
        .args(parse_conn_args(connstr))
        .output();

    if let Err(e) = &init {
        eprintln!("  pgbench init failed: {e}");
        return 0.0;
    }

    // Run benchmark
    let output = Command::new("pgbench")
        .args([
            "-c4", "-j2",
            &format!("-T{time}"),
            "--no-vacuum",
        ])
        .args(parse_conn_args(connstr))
        .arg(connstr_to_dbname(connstr))
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            parse_tps(&stdout)
        }
        Err(e) => {
            eprintln!("  pgbench failed: {e}");
            0.0
        }
    }
}

fn run_pgbench_select(connstr: &str, time: u32) -> f64 {
    let output = Command::new("pgbench")
        .args([
            "-c4", "-j2", "-S",
            &format!("-T{time}"),
        ])
        .args(parse_conn_args(connstr))
        .arg(connstr_to_dbname(connstr))
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            parse_tps(&stdout)
        }
        Err(e) => {
            eprintln!("  pgbench select-only failed: {e}");
            0.0
        }
    }
}

/// Parse TPS from pgbench output like: "tps = 1234.56 (without initial connection time)"
fn parse_tps(output: &str) -> f64 {
    for line in output.lines() {
        if line.contains("tps =") && line.contains("without") {
            if let Some(num_str) = line.split("tps =").nth(1) {
                if let Some(num) = num_str.trim().split_whitespace().next() {
                    return num.parse().unwrap_or(0.0);
                }
            }
        }
    }
    0.0
}

/// Extract dbname from connstr
fn connstr_to_dbname(connstr: &str) -> String {
    for part in connstr.split_whitespace() {
        if let Some(val) = part.strip_prefix("dbname=") {
            return val.to_string();
        }
    }
    "bench".to_string()
}

/// Convert connstr key=value pairs to pgbench CLI args
fn parse_conn_args(connstr: &str) -> Vec<String> {
    let mut args = Vec::new();
    for part in connstr.split_whitespace() {
        if let Some((key, val)) = part.split_once('=') {
            match key {
                "host" => { args.push("-h".to_string()); args.push(val.to_string()); }
                "port" => { args.push("-p".to_string()); args.push(val.to_string()); }
                "user" => { args.push("-U".to_string()); args.push(val.to_string()); }
                _ => {}
            }
        }
    }
    args
}
