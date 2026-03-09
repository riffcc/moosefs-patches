use std::env;
use std::net::TcpStream;
use std::process;
use std::time::Instant;

use moosefs_direct::{Client, ConnectOptions};

const BLOCK_SIZE: usize = 4096;
const DEFAULT_FILE_SIZE: u64 = 1 << 30;
const DEFAULT_OPS: u64 = 50_000;

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} <master:port> <path> <mode: read|write> [file-size-bytes] [ops] [password]"
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if !(args.len() == 4 || args.len() == 5 || args.len() == 6 || args.len() == 7) {
        eprintln!("{}", usage(args.first().map(String::as_str).unwrap_or("iops")));
        process::exit(2);
    }

    let master = &args[1];
    let path = &args[2];
    let mode = &args[3];
    let file_size = args
        .get(4)
        .map(|s| parse_u64(s, "file-size-bytes"))
        .unwrap_or(DEFAULT_FILE_SIZE);
    let ops = args
        .get(5)
        .map(|s| parse_u64(s, "ops"))
        .unwrap_or(DEFAULT_OPS);
    let env_password = env::var("MFS_PASSWORD").ok();
    let password = if args.len() == 7 {
        Some(args[6].as_str())
    } else {
        env_password.as_deref()
    };

    if file_size < BLOCK_SIZE as u64 {
        eprintln!("file size must be at least {}", BLOCK_SIZE);
        process::exit(2);
    }

    let max_in_flight = env::var("MFS_INFLIGHT")
        .ok()
        .map(|raw| parse_u64(&raw, "MFS_INFLIGHT"))
        .unwrap_or(64) as usize;
    let experimental_ooo = env_flag("MFS_OOO_WRITE_ACKS");
    let mut options = match password {
        Some(password) => ConnectOptions::default().with_password(password),
        None => ConnectOptions::default(),
    };
    if let Ok(subdir) = env::var("MFS_SUBDIR") {
        options = options.with_subdir(&subdir);
    }
    options = options
        .with_max_in_flight_write_fragments(max_in_flight)
        .with_experimental_ooo_write_acks(experimental_ooo);

    println!(
        "iops master={} path={} mode={} file_size={} ops={} block_size={} inflight={} experimental_ooo={}",
        master, path, mode, file_size, ops, BLOCK_SIZE, max_in_flight, experimental_ooo
    );

    let connect_start = Instant::now();
    let mut client = match Client::<TcpStream>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("connect/register failed: {err}");
            process::exit(1);
        }
    };
    let connect_elapsed = connect_start.elapsed();
    println!("connect/register: {:.3} ms", connect_elapsed.as_secs_f64() * 1000.0);

    let file = match client.ensure_file_len(path, file_size) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("prepare file failed: {err}");
            process::exit(1);
        }
    };

    let max_block = (file.size as usize / BLOCK_SIZE).max(1);
    let mut state = 0x4d_46_53_49_4f_50_53u64;
    let mut write_buf = vec![0u8; BLOCK_SIZE];
    let mut read_buf = vec![0u8; BLOCK_SIZE];

    if mode == "write" {
        fill_pattern(&mut write_buf, 0);
    }

    let start = Instant::now();
    for op_index in 0..ops {
        let block_index = (next_u64(&mut state) as usize) % max_block;
        let offset = (block_index * BLOCK_SIZE) as u64;
        match mode.as_str() {
            "read" => {
                if let Err(err) = client.read_at(&file, offset, &mut read_buf) {
                    eprintln!("read op {} failed: {}", op_index + 1, err);
                    process::exit(1);
                }
            }
            "write" => {
                fill_pattern(&mut write_buf, op_index);
                if let Err(err) = client.write_at(&file, offset, &write_buf) {
                    eprintln!("write op {} failed: {}", op_index + 1, err);
                    process::exit(1);
                }
            }
            _ => {
                eprintln!("mode must be 'read' or 'write'");
                process::exit(2);
            }
        }
    }
    let elapsed = start.elapsed();
    let iops = ops as f64 / elapsed.as_secs_f64();
    let mib_s = (ops as f64 * BLOCK_SIZE as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    let avg_us = elapsed.as_secs_f64() * 1_000_000.0 / ops as f64;

    println!(
        "summary ops={} elapsed_ms={:.3} iops={:.2} mib_s={:.2} avg_us={:.2}",
        ops,
        elapsed.as_secs_f64() * 1000.0,
        iops,
        mib_s,
        avg_us
    );
}

fn parse_u64(raw: &str, field: &str) -> u64 {
    match raw.parse::<u64>() {
        Ok(value) if value > 0 => value,
        _ => {
            eprintln!("invalid {}: {}", field, raw);
            process::exit(2);
        }
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|raw| matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"))
        .unwrap_or(false)
}

fn next_u64(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn fill_pattern(buf: &mut [u8], seed: u64) {
    let mut state = 0x43_48_55_4e_4b_49_4f_50u64 ^ seed;
    for byte in buf.iter_mut() {
        state = next_u64(&mut state);
        *byte = (state >> 24) as u8;
    }
}
