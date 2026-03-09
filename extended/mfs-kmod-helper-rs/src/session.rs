use std::io::{self, Cursor, Read, Write};
use std::net::TcpStream;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use thiserror::Error;

use crate::wire::{
    recv_packet, send_packet, CLTOMA_FUSE_REGISTER, FUSE_REGISTER_BLOB_ACL,
    MATOCL_FUSE_REGISTER, MFS_DEFAULT_MASTER_PORT, MFS_VERSMAJ, MFS_VERSMID,
    MFS_VERSMIN, REGISTER_NEWSESSION,
};

pub const HELPER_INFO_STR: &str = "mfskmod-helper";

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("master connection dropped")]
    ConnectionClosed,
    #[error("unexpected response type: expected {expected}, got {got}")]
    TypeMismatch { expected: u32, got: u32 },
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    #[error("master returned error status {0}")]
    MasterStatus(u8),
    #[error("session is not connected")]
    NotConnected,
}

pub struct MasterSession {
    stream: Option<TcpStream>,
    session_id: u32,
    next_msgid: u32,
}

impl MasterSession {
    pub fn disconnected() -> Self {
        Self {
            stream: None,
            session_id: 0,
            next_msgid: 1,
        }
    }

    pub fn connect(host: &str, port: u16) -> Result<Self, SessionError> {
        let mut session = Self::disconnected();
        let _ = session.connect_to(host, port, "/")?;
        Ok(session)
    }

    pub fn connect_to(
        &mut self,
        host: &str,
        port: u16,
        subdir: &str,
    ) -> Result<u32, SessionError> {
        let port = if port == 0 { MFS_DEFAULT_MASTER_PORT } else { port };
        let mut stream = TcpStream::connect((host, port))
            .map_err(SessionError::Io)
            .map_err(map_io)?;
        let _ = stream.set_nodelay(true);

        let session_id = register_over_stream(&mut stream, subdir).map_err(map_io)?;

        self.stream = Some(stream);
        self.session_id = session_id;
        self.next_msgid = 1;
        Ok(session_id)
    }

    pub fn session_id(&self) -> u32 {
        self.session_id
    }

    pub fn next_msgid(&mut self) -> u32 {
        self.next_msgid = self.next_msgid.wrapping_add(1);
        if self.next_msgid == 0 {
            self.next_msgid = 1;
        }
        self.next_msgid
    }

    pub fn rpc(
        &mut self,
        req_type: u32,
        req_data: &[u8],
    ) -> Result<(u32, Vec<u8>), SessionError> {
        let stream = self.stream_mut()?;
        rpc_over_stream(stream, req_type, req_data).map_err(map_io)
    }

    fn stream_mut(&mut self) -> Result<&mut TcpStream, SessionError> {
        self.stream.as_mut().ok_or(SessionError::NotConnected)
    }
}

fn register_over_stream<S: Read + Write>(
    stream: &mut S,
    subdir: &str,
) -> Result<u32, SessionError> {
    let register = build_register_packet(subdir);
    send_packet(stream, CLTOMA_FUSE_REGISTER, &register)?;

    let (rsp_type, rsp_data) = recv_packet(stream)?;
    if rsp_type != MATOCL_FUSE_REGISTER {
        return Err(SessionError::TypeMismatch {
            expected: MATOCL_FUSE_REGISTER,
            got: rsp_type,
        });
    }
    if rsp_data.len() == 1 {
        return Err(SessionError::MasterStatus(rsp_data[0]));
    }
    if rsp_data.len() < 8 {
        return Err(SessionError::Protocol("register response too short"));
    }

    let mut cur = Cursor::new(&rsp_data);
    let v1 = cur.read_u32::<BigEndian>()?;
    let v2 = cur.read_u32::<BigEndian>()?;
    let session_id = if (0x0001_0000..=0x0fff_ffff).contains(&v1) {
        v2
    } else {
        v1
    };

    Ok(session_id)
}

fn rpc_over_stream<S: Read + Write>(
    stream: &mut S,
    req_type: u32,
    req_data: &[u8],
) -> Result<(u32, Vec<u8>), SessionError> {
    send_packet(stream, req_type, req_data)?;
    let (rsp_type, rsp_data) = recv_packet(stream)?;

    let expected = req_type + 1;
    if rsp_type != expected {
        return Err(SessionError::TypeMismatch {
            expected,
            got: rsp_type,
        });
    }
    Ok((rsp_type, rsp_data))
}

fn build_register_packet(subdir: &str) -> Vec<u8> {
    let info = HELPER_INFO_STR.as_bytes();
    let info_len = (info.len() + 1) as u32;

    let subdir = if subdir.is_empty() { "/" } else { subdir };
    let subdir_b = subdir.as_bytes();
    let subdir_len = (subdir_b.len() + 1) as u32;

    let mut buf = Vec::with_capacity(
        FUSE_REGISTER_BLOB_ACL.len() + 1 + 4 + 4 + info_len as usize + 4 + subdir_len as usize,
    );

    buf.extend_from_slice(FUSE_REGISTER_BLOB_ACL);
    buf.push(REGISTER_NEWSESSION);
    buf.write_u16::<BigEndian>(MFS_VERSMAJ).expect("vec write");
    buf.push(MFS_VERSMID);
    buf.push(MFS_VERSMIN);
    buf.write_u32::<BigEndian>(info_len).expect("vec write");
    buf.extend_from_slice(info);
    buf.push(0);
    buf.write_u32::<BigEndian>(subdir_len).expect("vec write");
    buf.extend_from_slice(subdir_b);
    buf.push(0);

    buf
}

fn map_io(err: SessionError) -> SessionError {
    match err {
        SessionError::Io(ioe) => match ioe.kind() {
            io::ErrorKind::UnexpectedEof
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected => SessionError::ConnectionClosed,
            _ => SessionError::Io(ioe),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Read, Write};

    use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

    use super::*;
    use crate::wire::{recv_packet, send_packet, CLTOMA_FUSE_GETATTR, MATOCL_FUSE_GETATTR};

    #[derive(Default)]
    struct MockStream {
        reads: io::Cursor<Vec<u8>>,
        writes: Vec<u8>,
    }

    impl MockStream {
        fn with_read_data(data: Vec<u8>) -> Self {
            Self {
                reads: io::Cursor::new(data),
                writes: Vec::new(),
            }
        }
    }

    impl Read for MockStream {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.reads.read(buf)
        }
    }

    impl Write for MockStream {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn register_sends_register_newsess_with_acl_blob() {
        let mut srv_bytes = Vec::new();
        let mut rsp = Vec::new();
        rsp.write_u32::<BigEndian>(0x0001_0000).unwrap();
        rsp.write_u32::<BigEndian>(0xAABB_CCDD).unwrap();
        send_packet(&mut srv_bytes, MATOCL_FUSE_REGISTER, &rsp).unwrap();

        let mut stream = MockStream::with_read_data(srv_bytes);
        let sid = register_over_stream(&mut stream, "/").unwrap();
        assert_eq!(sid, 0xAABB_CCDD);

        let mut written = io::Cursor::new(&stream.writes);
        let (pkt_type, data) = recv_packet(&mut written).unwrap();
        assert_eq!(pkt_type, CLTOMA_FUSE_REGISTER);
        assert!(data.len() >= 64 + 1 + 4 + 4 + 4 + 2);
        assert_eq!(&data[..64], FUSE_REGISTER_BLOB_ACL);
        assert_eq!(data[64], REGISTER_NEWSESSION);

        let mut cur = std::io::Cursor::new(&data[65..]);
        assert_eq!(cur.read_u16::<BigEndian>().unwrap(), MFS_VERSMAJ);
        assert_eq!(cur.read_u8().unwrap(), MFS_VERSMID);
        assert_eq!(cur.read_u8().unwrap(), MFS_VERSMIN);
    }

    #[test]
    fn rpc_reports_type_mismatch() {
        let mut srv_bytes = Vec::new();
        send_packet(&mut srv_bytes, MATOCL_FUSE_GETATTR + 10, &[1, 2, 3]).unwrap();
        let mut stream = MockStream::with_read_data(srv_bytes);

        let err = rpc_over_stream(&mut stream, CLTOMA_FUSE_GETATTR, &[0, 1]).unwrap_err();
        match err {
            SessionError::TypeMismatch { expected, got } => {
                assert_eq!(expected, MATOCL_FUSE_GETATTR);
                assert_eq!(got, MATOCL_FUSE_GETATTR + 10);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn rpc_reports_connection_closed() {
        let mut stream = MockStream::with_read_data(vec![0, 1]);
        let err = rpc_over_stream(&mut stream, CLTOMA_FUSE_GETATTR, &[9, 9]).unwrap_err();
        let err = map_io(err);
        assert!(matches!(err, SessionError::ConnectionClosed));
    }

    #[test]
    fn register_uses_root_subdir_when_empty() {
        let pkt = build_register_packet("");
        let mut cur = std::io::Cursor::new(&pkt);
        let mut acl = [0u8; 64];
        cur.read_exact(&mut acl).unwrap();
        assert_eq!(&acl, FUSE_REGISTER_BLOB_ACL);
        let _register_cmd = cur.read_u8().unwrap();
        let _maj = cur.read_u16::<BigEndian>().unwrap();
        let _mid = cur.read_u8().unwrap();
        let _min = cur.read_u8().unwrap();

        let info_len = cur.read_u32::<BigEndian>().unwrap() as usize;
        let mut _info = vec![0u8; info_len];
        cur.read_exact(&mut _info).unwrap();

        let sub_len = cur.read_u32::<BigEndian>().unwrap() as usize;
        let mut sub = vec![0u8; sub_len];
        cur.read_exact(&mut sub).unwrap();

        assert_eq!(sub_len, 2);
        assert_eq!(&sub, b"/\0");
    }
}
