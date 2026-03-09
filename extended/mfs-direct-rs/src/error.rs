use std::fmt;
use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Protocol(&'static str),
    ProtocolDetail(String),
    InvalidInput(&'static str),
    Unsupported(&'static str),
    MoosefsStatus { op: &'static str, status: u8 },
}

impl Error {
    pub fn errno_like(&self) -> i32 {
        match self {
            Self::Io(err) => err.raw_os_error().unwrap_or(5),
            Self::Protocol(_) => 71,
            Self::ProtocolDetail(_) => 71,
            Self::InvalidInput(_) => 22,
            Self::Unsupported(_) => 95,
            Self::MoosefsStatus { status, .. } => mfs_status_to_errno(*status),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "I/O error: {err}"),
            Self::Protocol(msg) => write!(f, "protocol error: {msg}"),
            Self::ProtocolDetail(msg) => write!(f, "protocol error: {msg}"),
            Self::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Self::Unsupported(msg) => write!(f, "unsupported: {msg}"),
            Self::MoosefsStatus { op, status } => {
                write!(f, "MooseFS {op} returned status {status}")
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn mfs_status_to_errno(status: u8) -> i32 {
    match status {
        0 => 0,
        1 => 1,
        2 => 20,
        3 => 2,
        4 => 13,
        5 => 17,
        6 => 22,
        7 => 39,
        8 => 6,
        9 => 12,
        10 => 22,
        11 => 11,
        12 => 28,
        13 => 2,
        14 => 11,
        15 => 13,
        21 => 28,
        22 => 5,
        33 => 30,
        34 => 122,
        35 => 13,
        38 => 61,
        39 => 95,
        40 => 34,
        58 => 36,
        59 => 31,
        60 => 110,
        61 => 9,
        62 => 27,
        63 => 21,
        _ => 5,
    }
}

#[cfg(test)]
mod tests {
    use super::mfs_status_to_errno;

    #[test]
    fn known_statuses_map_to_errno_values() {
        assert_eq!(mfs_status_to_errno(0), 0);
        assert_eq!(mfs_status_to_errno(3), 2);
        assert_eq!(mfs_status_to_errno(12), 28);
        assert_eq!(mfs_status_to_errno(39), 95);
    }
}
