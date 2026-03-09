use std::env;
use std::net::TcpStream;
use std::process;

use moosefs_direct::{probe_quic_endpoint, Client, ConnectOptions, PacketModeMaster};

fn usage(program: &str) -> String {
    format!(
        "usage:\n  {program} write <master:port> <path> <data> [password]\n  {program} read <master:port> <path> [password]\n  {program} stat <master:port> <path> [password]\n  {program} quicprobe <master:port>\n  {program} quicstat <master:port> <path> [password]"
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("{}", usage(args.first().map(String::as_str).unwrap_or("smoke")));
        process::exit(2);
    }

    let command = args[1].as_str();
    let master = &args[2];
    let path = args.get(3).map(String::as_str).unwrap_or("/");

    let env_password = env::var("MFS_PASSWORD").ok();
    let password = match command {
        "write" if args.len() == 6 => Some(args[5].as_str()),
        "read" if args.len() == 5 => Some(args[4].as_str()),
        _ => env_password.as_deref(),
    };
    let mut options = match password {
        Some(password) => ConnectOptions::default().with_password(password),
        None => ConnectOptions::default(),
    };
    if let Ok(raw) = env::var("MFS_INFLIGHT") {
        let inflight = raw.parse::<usize>().unwrap_or(1).max(1);
        options = options.with_max_in_flight_write_fragments(inflight);
    }
    if let Ok(raw) = env::var("MFS_OOO_WRITE_ACKS") {
        let enabled = matches!(raw.as_str(), "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON");
        options = options.with_experimental_ooo_write_acks(enabled);
    }
    if command == "quicprobe" {
        match probe_quic_endpoint(master, &options.quic) {
            Ok(info) => {
                println!(
                    "server_version=0x{:06x} flags=0x{:08x} tcp_port={} udp_port={} max_datagram={} tls={} alpn={}",
                    info.server_version,
                    info.flags,
                    info.tcp_port,
                    info.udp_port,
                    info.max_datagram,
                    info.supports_tls_quic(),
                    String::from_utf8_lossy(&info.alpn)
                );
                return;
            }
            Err(err) => {
                eprintln!("quic probe failed: {err}");
                process::exit(1);
            }
        }
    }
    if command == "quicstat" {
        let mut client = match Client::<PacketModeMaster>::connect_registered(master, options) {
            Ok(client) => client,
            Err(err) => {
                eprintln!("quic connect/register failed: {err}");
                process::exit(1);
            }
        };
        match client.lookup_path(path) {
            Ok(inode) => {
                println!("inode={inode}");
                return;
            }
            Err(err) => {
                eprintln!("quic stat failed: {err}");
                process::exit(1);
            }
        }
    }
    let mut client = match Client::<TcpStream>::connect_registered(master, options) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("connect/register failed: {err}");
            process::exit(1);
        }
    };

    match command {
        "write" => {
            if !(args.len() == 5 || args.len() == 6) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            let data = args[4].as_bytes();
            if let Err(err) = client.write_all(path, data) {
                eprintln!("write failed: {err}");
                process::exit(1);
            }
            println!("wrote {} bytes to {}", data.len(), path);
        }
        "read" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.read_all(path) {
                Ok(data) => {
                    println!("{}", String::from_utf8_lossy(&data));
                }
                Err(err) => {
                    eprintln!("read failed: {err}");
                    process::exit(1);
                }
            }
        }
        "stat" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.lookup_path(path) {
                Ok(inode) => println!("inode={inode}"),
                Err(err) => {
                    eprintln!("stat failed: {err}");
                    process::exit(1);
                }
            }
        }
        "ensure" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.ensure_file(path, 0) {
                Ok(inode) => println!("inode={inode}"),
                Err(err) => {
                    eprintln!("ensure failed: {err}");
                    process::exit(1);
                }
            }
        }
        "chunk" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.debug_write_chunk_location(path) {
                Ok(location) => println!("{location:?}"),
                Err(err) => {
                    eprintln!("chunk failed: {err}");
                    process::exit(1);
                }
            }
        }
        "probe" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.debug_probe_write_setup(path) {
                Ok(steps) => {
                    for step in steps {
                        println!("{step}");
                    }
                }
                Err(err) => {
                    eprintln!("probe failed: {err}");
                    process::exit(1);
                }
            }
        }
        "gattr" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.debug_getattr(path) {
                Ok((inode, size, file_type)) => println!("inode={inode} size={size} type={file_type}"),
                Err(err) => {
                    eprintln!("gattr failed: {err}");
                    process::exit(1);
                }
            }
        }
        "open" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.debug_open_check(path) {
                Ok(inode) => println!("open ok inode={inode}"),
                Err(err) => {
                    eprintln!("open failed: {err}");
                    process::exit(1);
                }
            }
        }
        "trunc0" => {
            if !(args.len() == 4 || args.len() == 5) {
                eprintln!("{}", usage(&args[0]));
                process::exit(2);
            }
            match client.debug_truncate_zero(path) {
                Ok(inode) => println!("truncate ok inode={inode}"),
                Err(err) => {
                    eprintln!("truncate failed: {err}");
                    process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("{}", usage(&args[0]));
            process::exit(2);
        }
    }
}
