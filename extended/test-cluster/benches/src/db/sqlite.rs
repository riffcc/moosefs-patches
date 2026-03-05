use crate::report;
use crate::types::{BenchConfig, BenchResult, Target};
use crate::benchmarks::util;

use rusqlite::Connection;
use std::time::Instant;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let ops = config.sqlite_ops();
    report::section(&format!("11. SQLite ({ops} ops, journal + WAL modes)"));

    let mut results = Vec::new();

    // Test both journal modes
    for mode in ["DELETE", "WAL"] {
        let mut local_vals = [0f64; 3]; // insert, select, update
        let mut mfs_vals = [0f64; 3];

        for target in [Target::Local, Target::MooseFS] {
            let dir = util::target_dir(config, target, &format!("sqlite-{}", mode.to_lowercase()));
            util::cleanup(&dir);
            let db_path = dir.join("test.db");

            let conn = match Connection::open(&db_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("  SQLite open failed: {e}");
                    continue;
                }
            };

            // Set journal mode
            conn.execute_batch(&format!("PRAGMA journal_mode={mode};")).ok();
            conn.execute_batch("PRAGMA synchronous=FULL;").ok();

            // Create table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS bench (id INTEGER PRIMARY KEY, val TEXT, num REAL)",
                [],
            ).ok();

            // --- Insert benchmark ---
            let start = Instant::now();
            conn.execute_batch("BEGIN").ok();
            for i in 0..ops {
                conn.execute(
                    "INSERT INTO bench (val, num) VALUES (?1, ?2)",
                    rusqlite::params![format!("value_{i}"), i as f64 * 1.1],
                ).ok();
            }
            conn.execute_batch("COMMIT").ok();
            let insert_ops = ops as f64 / start.elapsed().as_secs_f64();

            // --- Select benchmark ---
            let start = Instant::now();
            for i in 0..ops {
                let id = (i % ops) + 1;
                let mut stmt = conn.prepare_cached(
                    "SELECT val, num FROM bench WHERE id = ?1"
                ).unwrap();
                let _: Option<(String, f64)> = stmt.query_row(
                    rusqlite::params![id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                ).ok();
            }
            let select_ops = ops as f64 / start.elapsed().as_secs_f64();

            // --- Update benchmark ---
            let start = Instant::now();
            conn.execute_batch("BEGIN").ok();
            for i in 0..ops {
                let id = (i % ops) + 1;
                conn.execute(
                    "UPDATE bench SET num = ?1 WHERE id = ?2",
                    rusqlite::params![i as f64 * 2.2, id],
                ).ok();
            }
            conn.execute_batch("COMMIT").ok();
            let update_ops = ops as f64 / start.elapsed().as_secs_f64();

            match target {
                Target::Local => {
                    local_vals[0] = insert_ops;
                    local_vals[1] = select_ops;
                    local_vals[2] = update_ops;
                }
                Target::MooseFS => {
                    mfs_vals[0] = insert_ops;
                    mfs_vals[1] = select_ops;
                    mfs_vals[2] = update_ops;
                }
            }

            util::cleanup(&dir);
        }

        results.push(BenchResult::new(
            &format!("SQLite Insert ({mode})"), local_vals[0], mfs_vals[0], "ops/s", true,
        ));
        results.push(BenchResult::new(
            &format!("SQLite Select ({mode})"), local_vals[1], mfs_vals[1], "ops/s", true,
        ));
        results.push(BenchResult::new(
            &format!("SQLite Update ({mode})"), local_vals[2], mfs_vals[2], "ops/s", true,
        ));
    }

    results
}
