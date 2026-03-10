use std::env;
use std::net::TcpStream;
use std::process;
use std::time::{Duration, Instant};

use moosefs_direct::{Client, ConnectOptions, ObjectLayout, ObjectStore};

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} <master:port> <object-root> <object-id> <size-bytes> [iterations] [password]\n\nexample:\n  {program} 10.7.1.195:19521 /objects bench-64m 67108864 3"
    )
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!(
            "warning: running object_bench in debug mode; prefer `cargo run --release --example object_bench -- ...`"
        );
    }

    let args: Vec<String> = env::args().collect();
    if !(args.len() == 5 || args.len() == 6 || args.len() == 7) {
        eprintln!(
            "{}",
            usage(args.first().map(String::as_str).unwrap_or("object_bench"))
        );
        process::exit(2);
    }

    let master = &args[1];
    let root = &args[2];
    let object_id = &args[3];
    let size = parse_u64(&args[4], "size-bytes") as usize;
    let iterations = args
        .get(5)
        .map(|raw| parse_u64(raw, "iterations"))
        .unwrap_or(3)
        .max(1) as usize;

    let env_password = env::var("MFS_PASSWORD").ok();
    let password = if args.len() == 7 {
        Some(args[6].as_str())
    } else {
        env_password.as_deref()
    };

    let options = match password {
        Some(password) => ConnectOptions::default().with_password(password),
        None => ConnectOptions::default(),
    };

    let connect_started = Instant::now();
    let client = match Client::<TcpStream>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("connect/register failed: {err}");
            process::exit(1);
        }
    };
    let connect_elapsed = connect_started.elapsed();

    let layout = match ObjectLayout::new(root) {
        Ok(layout) => layout,
        Err(err) => {
            eprintln!("invalid object root: {err}");
            process::exit(2);
        }
    };

    let mut store = ObjectStore::new(client, layout);
    let object_path = match store.object_path(object_id) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("invalid object id: {err}");
            process::exit(2);
        }
    };
    let payload = make_payload(size);

    println!(
        "object-bench master={} root={} object_id={} object_path={} size={} iterations={}",
        master, root, object_id, object_path, size, iterations
    );
    println!("connect/register: {:.3} ms", millis(connect_elapsed));

    let mut write_samples = Vec::with_capacity(iterations);
    let mut read_samples = Vec::with_capacity(iterations);

    for iteration in 0..iterations {
        let write_started = Instant::now();
        if let Err(err) = store.write_object(object_id, &payload) {
            eprintln!("write failed on iteration {}: {err}", iteration + 1);
            process::exit(1);
        }
        let write_elapsed = write_started.elapsed();
        write_samples.push(write_elapsed);

        let read_started = Instant::now();
        let read_back = match store.read_object(object_id) {
            Ok(bytes) => bytes,
            Err(err) => {
                eprintln!("read failed on iteration {}: {err}", iteration + 1);
                process::exit(1);
            }
        };
        let read_elapsed = read_started.elapsed();
        read_samples.push(read_elapsed);

        if read_back != payload {
            eprintln!("verification mismatch on iteration {}", iteration + 1);
            process::exit(1);
        }

        println!(
            "iteration={} write_ms={:.3} write_mib_s={:.2} read_ms={:.3} read_mib_s={:.2}",
            iteration + 1,
            millis(write_elapsed),
            throughput_mib_s(size, write_elapsed),
            millis(read_elapsed),
            throughput_mib_s(size, read_elapsed),
        );
    }

    let write_avg = average_duration(&write_samples);
    let read_avg = average_duration(&read_samples);
    println!(
        "summary write_avg_ms={:.3} write_avg_mib_s={:.2} read_avg_ms={:.3} read_avg_mib_s={:.2}",
        millis(write_avg),
        throughput_mib_s(size, write_avg),
        millis(read_avg),
        throughput_mib_s(size, read_avg),
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

fn average_duration(samples: &[Duration]) -> Duration {
    if samples.is_empty() {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(
        samples
            .iter()
            .map(Duration::as_secs_f64)
            .sum::<f64>()
            / samples.len() as f64,
    )
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn throughput_mib_s(size_bytes: usize, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs == 0.0 {
        return 0.0;
    }
    (size_bytes as f64 / 1_048_576.0) / secs
}

fn make_payload(size: usize) -> Vec<u8> {
    let mut out = vec![0u8; size];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = ((index as u64 * 1315423911 + 0x5a) & 0xff) as u8;
    }
    out
}
