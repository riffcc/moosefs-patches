use crate::report;
use crate::types::{BenchConfig, BenchResult};

use std::time::Instant;

pub fn run(config: &BenchConfig) -> Vec<BenchResult> {
    let ops = config.etcd_ops();
    report::section(&format!("12. etcd ({ops} operations)"));

    // Use tokio runtime for async etcd client
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");

    let local_endpoint = std::env::var("BENCH_ETCD_LOCAL")
        .unwrap_or_else(|_| "http://localhost:2379".to_string());
    let mfs_endpoint = std::env::var("BENCH_ETCD_MFS")
        .unwrap_or_else(|_| "http://localhost:2381".to_string());

    let local_vals = rt.block_on(run_etcd_bench(&local_endpoint, ops));
    let mfs_vals = rt.block_on(run_etcd_bench(&mfs_endpoint, ops));

    vec![
        BenchResult::new("etcd Put", local_vals[0], mfs_vals[0], "ops/s", true),
        BenchResult::new("etcd Get", local_vals[1], mfs_vals[1], "ops/s", true),
        BenchResult::new("etcd Range (100)", local_vals[2], mfs_vals[2], "ops/s", true),
        BenchResult::new("etcd Txn CAS", local_vals[3], mfs_vals[3], "ops/s", true),
    ]
}

async fn run_etcd_bench(endpoint: &str, ops: usize) -> [f64; 4] {
    let client = match etcd_client::Client::connect([endpoint], None).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  etcd connect to {endpoint} failed: {e}");
            return [0.0; 4];
        }
    };

    let mut kv = client.kv_client();
    let value = "v".repeat(256); // 256-byte values

    // --- Put ---
    let start = Instant::now();
    for i in 0..ops {
        let key = format!("bench/key/{i:08}");
        if let Err(e) = kv.put(key, value.clone(), None).await {
            eprintln!("  etcd put error: {e}");
            break;
        }
    }
    let put_ops = ops as f64 / start.elapsed().as_secs_f64();

    // --- Get (point lookups) ---
    let start = Instant::now();
    for i in 0..ops {
        let key = format!("bench/key/{:08}", i % ops);
        if let Err(e) = kv.get(key, None).await {
            eprintln!("  etcd get error: {e}");
            break;
        }
    }
    let get_ops = ops as f64 / start.elapsed().as_secs_f64();

    // --- Range (prefix scan, 100 keys at a time) ---
    let range_iters = ops / 10; // fewer iterations since each returns many keys
    let start = Instant::now();
    for i in 0..range_iters {
        let prefix = format!("bench/key/{:05}", (i * 100) % ops);
        let opts = etcd_client::GetOptions::new()
            .with_prefix()
            .with_limit(100);
        if let Err(e) = kv.get(prefix, Some(opts)).await {
            eprintln!("  etcd range error: {e}");
            break;
        }
    }
    let range_ops = range_iters as f64 / start.elapsed().as_secs_f64();

    // --- Txn (compare-and-swap) ---
    let txn_ops_count = ops / 2;
    let start = Instant::now();
    for i in 0..txn_ops_count {
        let key = format!("bench/key/{:08}", i % ops);
        let new_val = format!("updated_{i}");

        let txn = etcd_client::Txn::new()
            .when([etcd_client::Compare::value(key.clone(), etcd_client::CompareOp::NotEqual, "")])
            .and_then([etcd_client::TxnOp::put(key.clone(), new_val, None)])
            .or_else([etcd_client::TxnOp::put(key, value.clone(), None)]);

        if let Err(e) = kv.txn(txn).await {
            eprintln!("  etcd txn error: {e}");
            break;
        }
    }
    let txn_rate = txn_ops_count as f64 / start.elapsed().as_secs_f64();

    // Cleanup
    let _ = kv.delete("bench/", Some(etcd_client::DeleteOptions::new().with_prefix())).await;

    [put_ops, get_ops, range_ops, txn_rate]
}
