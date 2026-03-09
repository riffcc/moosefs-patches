use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process;
use std::time::{Duration, Instant};

use moosefs_direct::{Client, ConnectOptions, PacketModeMaster};
#[cfg(feature = "quic")]
use moosefs_direct::QuicStreamMaster;

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} <master:port> <path> [iterations] [password]\n\nexample:\n  {program} 127.0.0.1:19621 /moosefs-direct-smoke.txt 1000"
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if !(args.len() == 3 || args.len() == 4 || args.len() == 5) {
        eprintln!("{}", usage(args.first().map(String::as_str).unwrap_or("control_bench")));
        process::exit(2);
    }

    let master = &args[1];
    let path = &args[2];
    let iterations = args
        .get(3)
        .map(|raw| parse_usize(raw, "iterations"))
        .unwrap_or(1_000)
        .max(1);

    let env_password = env::var("MFS_PASSWORD").ok();
    let password = if args.len() == 5 {
        Some(args[4].as_str())
    } else {
        env_password.as_deref()
    };
    let options = match password {
        Some(password) => ConnectOptions::default().with_password(password),
        None => ConnectOptions::default(),
    };

    println!(
        "control-bench master={} path={} iterations={}",
        master, path, iterations
    );

    bench_tcp(master, path, iterations, options.clone());
    bench_packet(master, path, iterations, options.clone());
    #[cfg(feature = "quic")]
    bench_real_quic(master, path, iterations, options);
    #[cfg(not(feature = "quic"))]
    println!("real_quic skipped: build without --features quic");
}

fn bench_tcp(master: &str, path: &str, iterations: usize, options: ConnectOptions) {
    run_bench::<TcpStream, _>("tcp", iterations, || {
        Client::<TcpStream>::connect_registered(master, options.clone())
    }, path);
}

fn bench_packet(master: &str, path: &str, iterations: usize, options: ConnectOptions) {
    run_bench::<PacketModeMaster, _>("packet_udp", iterations, || {
        Client::<PacketModeMaster>::connect_registered(master, options.clone())
    }, path);
}

#[cfg(feature = "quic")]
fn bench_real_quic(master: &str, path: &str, iterations: usize, options: ConnectOptions) {
    run_bench::<QuicStreamMaster, _>("real_quic", iterations, || {
        Client::<QuicStreamMaster>::connect_registered(master, options.clone())
    }, path);
}

fn run_bench<S, F>(label: &str, iterations: usize, connect: F, path: &str)
where
    S: Read + Write,
    F: FnOnce() -> moosefs_direct::Result<Client<S>>,
{
    let connect_start = Instant::now();
    let mut client = match connect() {
        Ok(client) => client,
        Err(err) => {
            println!("{label} failed: {err}");
            return;
        }
    };
    let connect_elapsed = connect_start.elapsed();

    let mut lookup_total = Duration::ZERO;
    let mut getattr_total = Duration::ZERO;
    let mut inode = 0u64;
    for _ in 0..iterations {
        let lookup_start = Instant::now();
        inode = match client.lookup_path(path) {
            Ok(inode) => inode,
            Err(err) => {
                println!("{label} lookup failed: {err}");
                return;
            }
        };
        lookup_total += lookup_start.elapsed();

        let getattr_start = Instant::now();
        if let Err(err) = client.debug_getattr(path) {
            println!("{label} getattr failed: {err}");
            return;
        }
        getattr_total += getattr_start.elapsed();
    }

    let lookup_avg = div_duration(lookup_total, iterations as u32);
    let getattr_avg = div_duration(getattr_total, iterations as u32);
    println!(
        "{label} inode={} connect_ms={:.3} lookup_avg_us={:.2} getattr_avg_us={:.2} roundtrip_avg_us={:.2} lookup_ops_s={:.0} getattr_ops_s={:.0}",
        inode,
        millis(connect_elapsed),
        micros(lookup_avg),
        micros(getattr_avg),
        micros(lookup_avg + getattr_avg),
        ops_per_sec(lookup_avg),
        ops_per_sec(getattr_avg),
    );
}

fn parse_usize(raw: &str, field: &str) -> usize {
    match raw.parse::<usize>() {
        Ok(value) if value > 0 => value,
        _ => {
            eprintln!("invalid {}: {}", field, raw);
            process::exit(2);
        }
    }
}

fn div_duration(total: Duration, divisor: u32) -> Duration {
    if divisor == 0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(total.as_secs_f64() / divisor as f64)
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn ops_per_sec(duration: Duration) -> f64 {
    let secs = duration.as_secs_f64();
    if secs == 0.0 {
        0.0
    } else {
        1.0 / secs
    }
}
