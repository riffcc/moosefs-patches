use std::env;
use std::io::Read;
use std::net::TcpStream;
use std::process;
use std::time::{Duration, Instant};

use moosefs_direct::{Client, ConnectOptions};
#[cfg(feature = "quic")]
use moosefs_direct::QuicStreamMaster;

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} <master:port> <path-prefix> <sizes-bytes> <inflights> [iterations]\n\nexample:\n  {program} 10.7.1.195:19521 /matrix 67108864,524288000 16,32 2"
    )
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!(
            "warning: running bench_matrix in debug mode; use `cargo run --release --example bench_matrix -- ...` for real numbers"
        );
    }

    let args: Vec<String> = env::args().collect();
    if !(args.len() == 5 || args.len() == 6) {
        eprintln!(
            "{}",
            usage(args.first().map(String::as_str).unwrap_or("bench_matrix"))
        );
        process::exit(2);
    }

    let master = &args[1];
    let path_prefix = &args[2];
    let sizes = parse_list(&args[3], "sizes-bytes");
    let inflights = parse_list(&args[4], "inflights");
    let iterations = args
        .get(5)
        .map(|raw| parse_u64(raw, "iterations"))
        .unwrap_or(1)
        .max(1) as usize;

    let transport = env::var("MFS_MASTER_TRANSPORT")
        .unwrap_or_else(|_| "tcp".to_string())
        .to_lowercase();
    let force_buffered = env_flag("MFS_FORCE_BUFFERED");
    let warmup = env::var("MFS_WARMUP")
        .ok()
        .map(|raw| parse_u64(&raw, "MFS_WARMUP"))
        .unwrap_or(0) as usize;
    let password = env::var("MFS_PASSWORD").ok();

    println!("transport,build,payload_mode,size_bytes,inflight,warmup,iterations,connect_ms,write_avg_ms,write_avg_mib_s,read_avg_ms,read_avg_mib_s");

    for &size in &sizes {
        for &inflight in &inflights {
            let payload_mode = if force_buffered || size < 256 * 1024 * 1024 {
                "buffered"
            } else {
                "streaming"
            };

            let mut options = ConnectOptions::default()
                .with_max_in_flight_write_fragments(inflight as usize)
                .with_experimental_ooo_write_acks(env_flag("MFS_OOO_WRITE_ACKS"));
            if let Some(password) = password.as_deref() {
                options = options.with_password(password);
            }

            let path = format!(
                "{prefix}-{transport}-{payload}-s{size}-i{inflight}",
                prefix = path_prefix.trim_end_matches('/'),
                transport = transport,
                payload = payload_mode,
                size = size,
                inflight = inflight
            );

            match transport.as_str() {
                "tcp" => {
                    let result = run_case_tcp(master, &path, size as usize, iterations, warmup, options);
                    print_row("tcp", payload_mode, size, inflight, warmup, iterations, result);
                }
                "quic" => {
                    #[cfg(feature = "quic")]
                    {
                        let result =
                            run_case_quic(master, &path, size as usize, iterations, warmup, options);
                        print_row("quic", payload_mode, size, inflight, warmup, iterations, result);
                    }
                    #[cfg(not(feature = "quic"))]
                    {
                        eprintln!("MFS_MASTER_TRANSPORT=quic requires --features quic");
                        process::exit(2);
                    }
                }
                other => {
                    eprintln!("unsupported MFS_MASTER_TRANSPORT: {other}");
                    process::exit(2);
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct CaseResult {
    connect: Duration,
    write_avg: Duration,
    read_avg: Duration,
}

fn print_row(
    transport: &str,
    payload_mode: &str,
    size: u64,
    inflight: u64,
    warmup: usize,
    iterations: usize,
    result: CaseResult,
) {
    let build = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    println!(
        "{transport},{build},{payload_mode},{size},{inflight},{warmup},{iterations},{connect_ms:.3},{write_ms:.3},{write_mib:.2},{read_ms:.3},{read_mib:.2}",
        connect_ms = millis(result.connect),
        write_ms = millis(result.write_avg),
        write_mib = mib_per_sec(size as usize, result.write_avg),
        read_ms = millis(result.read_avg),
        read_mib = mib_per_sec(size as usize, result.read_avg),
    );
}

fn run_case_tcp(
    master: &str,
    path: &str,
    size: usize,
    iterations: usize,
    warmup: usize,
    options: ConnectOptions,
) -> CaseResult {
    let connect_started = Instant::now();
    let mut client = match Client::<TcpStream>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("tcp connect/register failed: {err}");
            process::exit(1);
        }
    };
    let connect = connect_started.elapsed();
    drive_case(&mut client, path, size, iterations, warmup)
        .map(|(write_avg, read_avg)| CaseResult {
            connect,
            write_avg,
            read_avg,
        })
        .unwrap_or_else(|err| {
            eprintln!("tcp bench failed: {err}");
            process::exit(1);
        })
}

#[cfg(feature = "quic")]
fn run_case_quic(
    master: &str,
    path: &str,
    size: usize,
    iterations: usize,
    warmup: usize,
    options: ConnectOptions,
) -> CaseResult {
    let connect_started = Instant::now();
    let mut client = match Client::<QuicStreamMaster>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("quic connect/register failed: {err}");
            process::exit(1);
        }
    };
    let connect = connect_started.elapsed();
    drive_case(&mut client, path, size, iterations, warmup)
        .map(|(write_avg, read_avg)| CaseResult {
            connect,
            write_avg,
            read_avg,
        })
        .unwrap_or_else(|err| {
            eprintln!("quic bench failed: {err}");
            process::exit(1);
        })
}

fn drive_case<S>(
    client: &mut Client<S>,
    path: &str,
    size: usize,
    iterations: usize,
    warmup: usize,
) -> moosefs_direct::Result<(Duration, Duration)>
where
    S: Read + std::io::Write,
    Client<S>: MatrixBenchOps,
{
    let payload = make_payload(size);

    for _ in 0..warmup {
        client.matrix_write_all(path, &payload)?;
        let _ = client.matrix_read_all(path)?;
    }

    let mut write_total = Duration::ZERO;
    let mut read_total = Duration::ZERO;
    for _ in 0..iterations {
        let write_started = Instant::now();
        client.matrix_write_all(path, &payload)?;
        write_total += write_started.elapsed();

        let read_started = Instant::now();
        let read_back = client.matrix_read_all(path)?;
        read_total += read_started.elapsed();
        if read_back != payload {
            eprintln!("verification failed for {path}");
            process::exit(1);
        }
    }

    Ok((
        div_duration(write_total, iterations as u32),
        div_duration(read_total, iterations as u32),
    ))
}

trait MatrixBenchOps {
    fn matrix_write_all(&mut self, path: &str, bytes: &[u8]) -> moosefs_direct::Result<()>;
    fn matrix_read_all(&mut self, path: &str) -> moosefs_direct::Result<Vec<u8>>;
}

impl MatrixBenchOps for Client<TcpStream> {
    fn matrix_write_all(&mut self, path: &str, bytes: &[u8]) -> moosefs_direct::Result<()> {
        self.write_all(path, bytes)
    }

    fn matrix_read_all(&mut self, path: &str) -> moosefs_direct::Result<Vec<u8>> {
        self.read_all(path)
    }
}

#[cfg(feature = "quic")]
impl MatrixBenchOps for Client<QuicStreamMaster> {
    fn matrix_write_all(&mut self, path: &str, bytes: &[u8]) -> moosefs_direct::Result<()> {
        self.write_all(path, bytes)
    }

    fn matrix_read_all(&mut self, path: &str) -> moosefs_direct::Result<Vec<u8>> {
        self.read_all(path)
    }
}

fn parse_list(raw: &str, field: &str) -> Vec<u64> {
    let items: Vec<u64> = raw
        .split(',')
        .map(|item| parse_u64(item.trim(), field))
        .collect();
    if items.is_empty() {
        eprintln!("{} must not be empty", field);
        process::exit(2);
    }
    items
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

fn make_payload(size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = ((index as u64 * 1315423911u64 + 0x5a) & 0xff) as u8;
    }
    payload
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn mib_per_sec(size: usize, duration: Duration) -> f64 {
    if duration.is_zero() {
        return 0.0;
    }
    (size as f64 / (1024.0 * 1024.0)) / duration.as_secs_f64()
}

fn div_duration(duration: Duration, divisor: u32) -> Duration {
    if divisor == 0 {
        return Duration::ZERO;
    }
    Duration::from_secs_f64(duration.as_secs_f64() / divisor as f64)
}
