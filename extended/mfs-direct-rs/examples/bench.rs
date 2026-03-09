use std::env;
use std::fs::File;
use std::io::{self, Read};
use std::net::TcpStream;
use std::process;
use std::time::{Duration, Instant};

use memmap2::Mmap;
use moosefs_direct::{Client, ConnectOptions};
#[cfg(feature = "quic")]
use moosefs_direct::QuicStreamMaster;

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} <master:port> <path> <size-bytes> [iterations] [password]\n\nexample:\n  {program} 10.7.1.195:19521 /moosefs-direct-bench.bin 67108864 3"
    )
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!("warning: running bench in debug mode; throughput numbers are not representative. Prefer `cargo run --release --example bench -- ...`");
    }
    let args: Vec<String> = env::args().collect();
    if !(args.len() == 4 || args.len() == 5 || args.len() == 6) {
        eprintln!("{}", usage(args.first().map(String::as_str).unwrap_or("bench")));
        process::exit(2);
    }

    let master = &args[1];
    let path = &args[2];
    let size = parse_u64(&args[3], "size-bytes") as usize;
    let iterations = args
        .get(4)
        .map(|s| parse_u64(s, "iterations"))
        .unwrap_or(3)
        .max(1) as usize;

    let env_password = env::var("MFS_PASSWORD").ok();
    let password = if args.len() == 6 {
        Some(args[5].as_str())
    } else {
        env_password.as_deref()
    };
    let mut options = match password {
        Some(password) => ConnectOptions::default().with_password(password),
        None => ConnectOptions::default(),
    };
    let max_in_flight = env::var("MFS_INFLIGHT")
        .ok()
        .map(|raw| parse_u64(&raw, "MFS_INFLIGHT"))
        .unwrap_or(options.max_in_flight_write_fragments as u64) as usize;
    let experimental_ooo = env_flag("MFS_OOO_WRITE_ACKS");
    let force_buffered = env_flag("MFS_FORCE_BUFFERED");
    let transport = env::var("MFS_MASTER_TRANSPORT")
        .unwrap_or_else(|_| "tcp".to_string())
        .to_lowercase();
    let skip_write = env_flag("MFS_SKIP_WRITE");
    let warmup_iterations = env::var("MFS_WARMUP")
        .ok()
        .map(|raw| parse_u64(&raw, "MFS_WARMUP"))
        .unwrap_or(0) as usize;
    let streaming = if force_buffered {
        false
    } else {
        env_flag("MFS_STREAMING") || size >= (256 * 1024 * 1024)
    };
    options = options
        .with_max_in_flight_write_fragments(max_in_flight)
        .with_experimental_ooo_write_acks(experimental_ooo);

    let source_file = env::var("MFS_SOURCE_FILE").ok();
    let source_map = if streaming {
        None
    } else if let Some(path) = source_file.as_deref() {
        Some(map_source_file(path, size))
    } else {
        None
    };
    let payload = if streaming || source_map.is_some() {
        None
    } else {
        Some(make_payload(size))
    };
    let build_mode = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let payload_mode = if streaming {
        "streaming"
    } else if source_map.is_some() {
        "file"
    } else {
        "buffered"
    };

    println!(
        "benchmark master={} path={} size={} bytes iterations={} warmup={} inflight={} experimental_ooo={} build={} payload_mode={} streaming={} force_buffered={} skip_write={} source_file={}",
        master,
        path,
        size,
        iterations,
        warmup_iterations,
        max_in_flight,
        experimental_ooo,
        build_mode,
        payload_mode,
        streaming,
        force_buffered,
        skip_write,
        source_file.as_deref().unwrap_or("-"),
    );
    match transport.as_str() {
        "tcp" => run_bench_tcp(master, path, size, iterations, warmup_iterations, options, streaming, skip_write, source_map.as_deref(), payload.as_deref()),
        "quic" => {
            #[cfg(feature = "quic")]
            run_bench_quic(master, path, size, iterations, warmup_iterations, options, streaming, skip_write, source_map.as_deref(), payload.as_deref());
            #[cfg(not(feature = "quic"))]
            {
                eprintln!("MFS_MASTER_TRANSPORT=quic requires building with --features quic");
                process::exit(2);
            }
        }
        other => {
            eprintln!("unsupported MFS_MASTER_TRANSPORT: {other}");
            process::exit(2);
        }
    }
}

fn run_bench_tcp(
    master: &str,
    path: &str,
    size: usize,
    iterations: usize,
    warmup_iterations: usize,
    options: ConnectOptions,
    streaming: bool,
    skip_write: bool,
    source_map: Option<&[u8]>,
    payload: Option<&[u8]>,
) {
    let connect_start = Instant::now();
    let mut client = match Client::<TcpStream>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("connect/register failed: {err}");
            process::exit(1);
        }
    };
    let connect_elapsed = connect_start.elapsed();
    println!("connect/register: {:.3} ms", millis(connect_elapsed));
    drive_bench(
        &mut client,
        path,
        size,
        iterations,
        warmup_iterations,
        streaming,
        skip_write,
        source_map,
        payload,
    );
}

#[cfg(feature = "quic")]
fn run_bench_quic(
    master: &str,
    path: &str,
    size: usize,
    iterations: usize,
    warmup_iterations: usize,
    options: ConnectOptions,
    streaming: bool,
    skip_write: bool,
    source_map: Option<&[u8]>,
    payload: Option<&[u8]>,
) {
    let connect_start = Instant::now();
    let mut client = match Client::<QuicStreamMaster>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("quic connect/register failed: {err}");
            process::exit(1);
        }
    };
    let connect_elapsed = connect_start.elapsed();
    println!("connect/register: {:.3} ms", millis(connect_elapsed));
    drive_bench(
        &mut client,
        path,
        size,
        iterations,
        warmup_iterations,
        streaming,
        skip_write,
        source_map,
        payload,
    );
}

fn drive_bench<S>(
    client: &mut Client<S>,
    path: &str,
    size: usize,
    iterations: usize,
    warmup_iterations: usize,
    streaming: bool,
    skip_write: bool,
    source_map: Option<&[u8]>,
    payload: Option<&[u8]>,
) where
    S: Read + std::io::Write,
    Client<S>: BenchClientOps,
{
    for warmup in 0..warmup_iterations {
        if skip_write {
            break;
        }
        if streaming {
            let mut reader = PatternReader::new(size as u64);
            if let Err(err) = client.bench_write_from_reader(path, size as u64, &mut reader) {
                eprintln!("warmup write failed on iteration {}: {err}", warmup + 1);
                process::exit(1);
            }
        } else if let Some(source_map) = source_map {
            if let Err(err) = client.bench_write_all(path, source_map) {
                eprintln!("warmup write failed on iteration {}: {err}", warmup + 1);
                process::exit(1);
            }
        } else if let Some(payload) = payload {
            if let Err(err) = client.bench_write_all(path, payload) {
                eprintln!("warmup write failed on iteration {}: {err}", warmup + 1);
                process::exit(1);
            }
        }
        println!("warmup={} done", warmup + 1);
    }

    let mut write_samples = Vec::with_capacity(iterations);
    let mut read_samples = Vec::with_capacity(iterations);

    for iteration in 0..iterations {
        let write_elapsed = if skip_write {
            Duration::ZERO
        } else {
            let write_start = Instant::now();
            if streaming {
                let mut reader = PatternReader::new(size as u64);
                if let Err(err) = client.bench_write_from_reader(path, size as u64, &mut reader) {
                    eprintln!("streaming write failed on iteration {}: {err}", iteration + 1);
                    process::exit(1);
                }
            } else if let Some(source_map) = source_map {
                if let Err(err) = client.bench_write_all(path, source_map) {
                    eprintln!("write failed on iteration {}: {err}", iteration + 1);
                    process::exit(1);
                }
            } else if let Some(payload) = payload {
                if let Err(err) = client.bench_write_all(path, payload) {
                    eprintln!("write failed on iteration {}: {err}", iteration + 1);
                    process::exit(1);
                }
            };
            write_start.elapsed()
        };
        write_samples.push(write_elapsed);

        let read_elapsed = if streaming && !skip_write {
            Duration::ZERO
        } else {
            let read_start = Instant::now();
            let read_back = match client.bench_read_all(path) {
                Ok(data) => data,
                Err(err) => {
                    eprintln!("read failed on iteration {}: {err}", iteration + 1);
                    process::exit(1);
                }
            };
            let read_elapsed = read_start.elapsed();
            read_samples.push(read_elapsed);

            let expected = source_map.or(payload);
            if let Some(expected) = expected {
                if read_back != expected {
                    eprintln!("verification failed on iteration {}", iteration + 1);
                    process::exit(1);
                }
            }
            if skip_write && read_back.len() != size {
                eprintln!(
                    "read length mismatch on iteration {}: expected {} bytes, got {}",
                    iteration + 1,
                    size,
                    read_back.len()
                );
                process::exit(1);
            }
            read_elapsed
        };

        println!(
            "iter={} write_ms={:.3} write_mib_s={:.2} read_ms={:.3} read_mib_s={:.2}",
            iteration + 1,
            millis(write_elapsed),
            mib_per_sec(size, write_elapsed),
            millis(read_elapsed),
            mib_per_sec(size, read_elapsed),
        );
    }

    let write_total = total_duration(&write_samples);
    let read_total = total_duration(&read_samples);
    println!(
        "summary write_avg_ms={:.3} write_avg_mib_s={:.2} read_avg_ms={:.3} read_avg_mib_s={:.2}",
        millis(div_duration(write_total, iterations as u32)),
        mib_per_sec(size, div_duration(write_total, iterations as u32)),
        if read_samples.is_empty() {
            0.0
        } else {
            millis(div_duration(read_total, iterations as u32))
        },
        if read_samples.is_empty() {
            0.0
        } else {
            mib_per_sec(size, div_duration(read_total, iterations as u32))
        },
    );
}

trait BenchClientOps {
    fn bench_write_all(&mut self, path: &str, bytes: &[u8]) -> moosefs_direct::Result<()>;
    fn bench_write_from_reader<R: Read>(
        &mut self,
        path: &str,
        size: u64,
        reader: &mut R,
    ) -> moosefs_direct::Result<()>;
    fn bench_read_all(&mut self, path: &str) -> moosefs_direct::Result<Vec<u8>>;
}

impl BenchClientOps for Client<TcpStream> {
    fn bench_write_all(&mut self, path: &str, bytes: &[u8]) -> moosefs_direct::Result<()> {
        self.write_all(path, bytes)
    }

    fn bench_write_from_reader<R: Read>(
        &mut self,
        path: &str,
        size: u64,
        reader: &mut R,
    ) -> moosefs_direct::Result<()> {
        self.write_from_reader(path, size, reader)
    }

    fn bench_read_all(&mut self, path: &str) -> moosefs_direct::Result<Vec<u8>> {
        self.read_all(path)
    }
}

#[cfg(feature = "quic")]
impl BenchClientOps for Client<QuicStreamMaster> {
    fn bench_write_all(&mut self, path: &str, bytes: &[u8]) -> moosefs_direct::Result<()> {
        self.write_all(path, bytes)
    }

    fn bench_write_from_reader<R: Read>(
        &mut self,
        path: &str,
        size: u64,
        reader: &mut R,
    ) -> moosefs_direct::Result<()> {
        self.write_from_reader(path, size, reader)
    }

    fn bench_read_all(&mut self, path: &str) -> moosefs_direct::Result<Vec<u8>> {
        self.read_all(path)
    }
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

fn map_source_file(path: &str, size: usize) -> Mmap {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("failed to open source file {}: {}", path, err);
            process::exit(2);
        }
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(err) => {
            eprintln!("failed to stat source file {}: {}", path, err);
            process::exit(2);
        }
    };
    if metadata.len() < size as u64 {
        eprintln!(
            "source file {} is too small: need {} bytes, have {}",
            path,
            size,
            metadata.len()
        );
        process::exit(2);
    }
    match unsafe { Mmap::map(&file) } {
        Ok(map) => map,
        Err(err) => {
            eprintln!("failed to mmap source file {}: {}", path, err);
            process::exit(2);
        }
    }
}

struct PatternReader {
    remaining: u64,
    state: u64,
}

impl PatternReader {
    fn new(remaining: u64) -> Self {
        Self {
            remaining,
            state: 0x4d_46_53_42_45_4e_43_48u64,
        }
    }
}

impl Read for PatternReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let count = buf.len().min(self.remaining as usize);
        for byte in &mut buf[..count] {
            self.state ^= self.state << 13;
            self.state ^= self.state >> 7;
            self.state ^= self.state << 17;
            *byte = (self.state >> 24) as u8;
        }
        self.remaining -= count as u64;
        Ok(count)
    }
}

fn make_payload(size: usize) -> Vec<u8> {
    let mut state = 0x4d_46_53_42_45_4e_43_48u64;
    let mut out = Vec::with_capacity(size);
    for _ in 0..size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push((state >> 24) as u8);
    }
    out
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn mib_per_sec(bytes: usize, duration: Duration) -> f64 {
    let secs = duration.as_secs_f64();
    if secs == 0.0 {
        return f64::INFINITY;
    }
    bytes as f64 / (1024.0 * 1024.0) / secs
}

fn total_duration(samples: &[Duration]) -> Duration {
    samples
        .iter()
        .copied()
        .fold(Duration::ZERO, |acc, sample| acc + sample)
}

fn div_duration(duration: Duration, divisor: u32) -> Duration {
    Duration::from_secs_f64(duration.as_secs_f64() / divisor as f64)
}
