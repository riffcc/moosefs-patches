use std::env;
use std::fs::OpenOptions;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};

use mfskmod_helper::ctrl::{
    CtrlHdr, MFS_CTRL_FLAG_REQUEST, MFS_CTRL_MAGIC, MFS_CTRL_MAX_PAYLOAD,
    MFS_CTRL_VERSION,
};
use mfskmod_helper::handler::handle_request;
use mfskmod_helper::session::MasterSession;
use nix::sys::signal::{signal, SigHandler, Signal};

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_signal(_: i32) {
    STOP.store(true, Ordering::SeqCst);
}

#[derive(Debug, Clone)]
struct Config {
    foreground: bool,
    verbose: bool,
    device: String,
}

fn parse_args(args: &[String]) -> Result<Config, String> {
    let mut cfg = Config {
        foreground: false,
        verbose: false,
        device: "/dev/mfs_ctrl".to_string(),
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-f" => cfg.foreground = true,
            "-v" => cfg.verbose = true,
            "-d" => {
                i += 1;
                if i >= args.len() {
                    return Err("-d requires a path".to_string());
                }
                cfg.device = args[i].clone();
            }
            _ => {
                return Err(format!(
                    "usage: {} [-f] [-v] [-d /dev/mfs_ctrl]",
                    args.first().cloned().unwrap_or_else(|| "mfskmod-helper".to_string())
                ));
            }
        }
        i += 1;
    }

    Ok(cfg)
}

fn setup_logging(verbose: bool) {
    let mut builder = env_logger::Builder::from_default_env();
    if verbose {
        builder.filter_level(log::LevelFilter::Debug);
    } else {
        builder.filter_level(log::LevelFilter::Info);
    }
    let _ = builder.try_init();
}

fn install_signal_handlers() -> io::Result<()> {
    unsafe {
        signal(Signal::SIGINT, SigHandler::Handler(handle_signal))
            .map_err(io::Error::other)?;
        signal(Signal::SIGTERM, SigHandler::Handler(handle_signal))
            .map_err(io::Error::other)?;
    }
    Ok(())
}

fn read_exact_intr<R: Read>(reader: &mut R, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match reader.read(buf) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected EOF",
                ))
            }
            Ok(n) => {
                let (_, rest) = buf.split_at_mut(n);
                buf = rest;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn write_all_intr<W: Write>(writer: &mut W, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        match writer.write(buf) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "short write to ctrl device",
                ))
            }
            Ok(n) => buf = &buf[n..],
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

fn run(cfg: &Config) -> io::Result<()> {
    if !cfg.foreground {
        nix::unistd::daemon(false, false)
            .map_err(io::Error::other)?;
    }

    install_signal_handlers()?;

    let mut dev = OpenOptions::new().read(true).write(true).open(&cfg.device)?;
    let mut session = MasterSession::disconnected();

    while !STOP.load(Ordering::SeqCst) {
        let mut hdr_buf = [0u8; CtrlHdr::SIZE];
        if let Err(err) = read_exact_intr(&mut dev, &mut hdr_buf) {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                continue;
            }
            return Err(err);
        }

        let hdr = CtrlHdr::from_bytes(&hdr_buf)?;
        let magic = hdr.magic;
        let version = hdr.version;
        let flags = hdr.flags;
        let payload_len_u32 = hdr.payload_len;
        if magic != MFS_CTRL_MAGIC || version != MFS_CTRL_VERSION || (flags & MFS_CTRL_FLAG_REQUEST) == 0 {
            continue;
        }
        if payload_len_u32 > MFS_CTRL_MAX_PAYLOAD {
            continue;
        }

        let mut payload = vec![0u8; payload_len_u32 as usize];
        if !payload.is_empty() {
            read_exact_intr(&mut dev, &mut payload)?;
        }

        let (status, rsp_payload) = match handle_request(&hdr, &payload, &mut session) {
            Ok(p) => (0, p),
            Err(e) => {
                log::debug!("handle_request failed: {e}");
                (e.status_code(), Vec::new())
            }
        };

        let rsp_hdr = CtrlHdr::response(&hdr, status, rsp_payload.len() as u32);
        let rsp_hdr_bytes = rsp_hdr.to_bytes();
        write_all_intr(&mut dev, &rsp_hdr_bytes)?;
        if !rsp_payload.is_empty() {
            write_all_intr(&mut dev, &rsp_payload)?;
        }
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let cfg = match parse_args(&args) {
        Ok(cfg) => cfg,
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(2);
        }
    };

    setup_logging(cfg.verbose);

    if let Err(err) = run(&cfg) {
        eprintln!("fatal: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_defaults() {
        let args = vec!["mfskmod-helper".to_string()];
        let cfg = parse_args(&args).unwrap();
        assert!(!cfg.foreground);
        assert!(!cfg.verbose);
        assert_eq!(cfg.device, "/dev/mfs_ctrl");
    }

    #[test]
    fn parse_args_all_flags() {
        let args = vec![
            "mfskmod-helper".to_string(),
            "-f".to_string(),
            "-v".to_string(),
            "-d".to_string(),
            "/tmp/dev".to_string(),
        ];
        let cfg = parse_args(&args).unwrap();
        assert!(cfg.foreground);
        assert!(cfg.verbose);
        assert_eq!(cfg.device, "/tmp/dev");
    }

    #[test]
    fn parse_args_rejects_unknown() {
        let args = vec!["mfskmod-helper".to_string(), "-z".to_string()];
        assert!(parse_args(&args).is_err());
    }
}
