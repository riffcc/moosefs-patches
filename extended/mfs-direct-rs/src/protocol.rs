use std::io::{self, Read, Write};

use crate::error::{Error, Result};

pub const PACKET_HEADER_LEN: usize = 8;
pub const MAX_PACKET_SIZE: usize = 50_000_000;
pub const ANTOAN_NOP: u32 = 0;
pub const CHUNK_SIZE: u64 = 0x0400_0000;
pub const BLOCK_SIZE: u32 = 0x0001_0000;
pub const ROOT_INODE: u32 = 1;

pub const CLTOMA_FUSE_REGISTER: u32 = 400;
pub const MATOCL_FUSE_REGISTER: u32 = 401;
pub const CLTOMA_FUSE_LOOKUP: u32 = 406;
pub const MATOCL_FUSE_LOOKUP: u32 = 407;
pub const CLTOMA_FUSE_GETATTR: u32 = 408;
pub const MATOCL_FUSE_GETATTR: u32 = 409;
pub const CLTOMA_FUSE_MKDIR: u32 = 418;
pub const MATOCL_FUSE_MKDIR: u32 = 419;
pub const CLTOMA_FUSE_UNLINK: u32 = 420;
pub const MATOCL_FUSE_UNLINK: u32 = 421;
pub const CLTOMA_FUSE_RMDIR: u32 = 422;
pub const MATOCL_FUSE_RMDIR: u32 = 423;
pub const CLTOMA_FUSE_RENAME: u32 = 424;
pub const MATOCL_FUSE_RENAME: u32 = 425;
pub const CLTOMA_FUSE_OPEN: u32 = 430;
pub const MATOCL_FUSE_OPEN: u32 = 431;
pub const CLTOMA_FUSE_READ_CHUNK: u32 = 432;
pub const MATOCL_FUSE_READ_CHUNK: u32 = 433;
pub const CLTOMA_FUSE_WRITE_CHUNK: u32 = 434;
pub const MATOCL_FUSE_WRITE_CHUNK: u32 = 435;
pub const CLTOMA_FUSE_WRITE_CHUNK_END: u32 = 436;
pub const MATOCL_FUSE_WRITE_CHUNK_END: u32 = 437;
pub const CLTOMA_FUSE_TRUNCATE: u32 = 464;
pub const MATOCL_FUSE_TRUNCATE: u32 = 465;
pub const CLTOMA_FUSE_CREATE: u32 = 482;
pub const MATOCL_FUSE_CREATE: u32 = 483;
pub const CLTOMA_QUIC_HELLO: u32 = 538;
pub const MATOCL_QUIC_HELLO: u32 = 539;
pub const CLTOMA_QUIC_PACKET: u32 = 540;
pub const MATOCL_QUIC_PACKET: u32 = 541;

pub const CLTOCS_READ: u32 = 200;
pub const CSTOCL_READ_STATUS: u32 = 201;
pub const CSTOCL_READ_DATA: u32 = 202;
pub const CLTOCS_WRITE: u32 = 210;
pub const CSTOCL_WRITE_STATUS: u32 = 211;
pub const CLTOCS_WRITE_DATA: u32 = 212;
pub const CLTOCS_WRITE_FINISH: u32 = 213;

pub const REGISTER_GETRANDOM: u8 = 1;
pub const REGISTER_NEWSESSION: u8 = 2;
pub const REGISTER_RECONNECT: u8 = 3;
pub const DEFAULT_MASTER_PORT: u16 = 9421;
pub const VERSION_MAJOR: u16 = 4;
pub const VERSION_MID: u8 = 58;
pub const VERSION_MINOR: u8 = 3;
pub const CHUNKOPFLAG_CANMODTIME: u8 = 0x01;
pub const QUIC_HELLO_MAGIC: u32 = 0x4D46_5351;
pub const QUIC_HELLO_FLAG_PACKET_MODE: u32 = 0x0000_0001;
pub const QUIC_HELLO_FLAG_TCP_FALLBACK: u32 = 0x0000_0002;
pub const QUIC_HELLO_FLAG_ZERO_RTT: u32 = 0x0000_0004;
pub const QUIC_HELLO_FLAG_DATAGRAMS: u32 = 0x0000_0008;
pub const QUIC_HELLO_FLAG_TLS: u32 = 0x0000_0010;
pub const TYPE_FILE: u8 = 1;
pub const TYPE_DIRECTORY: u8 = 2;
pub const OPEN_READWRITE: u8 = 2;

pub const REGISTER_BLOB_ACL: &[u8; 64] =
    b"DjI1GAQDULI5d2YjA26ypc3ovkhjvhciTQVx3CS4nYgtBoUcsljiVpsErJENHaw0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub packet_type: u32,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn new(packet_type: u32, payload: Vec<u8>) -> Self {
        Self {
            packet_type,
            payload,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(PACKET_HEADER_LEN + self.payload.len());
        out.extend_from_slice(&self.packet_type.to_be_bytes());
        out.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }
}

pub fn write_packet<W: Write>(writer: &mut W, packet_type: u32, payload: &[u8]) -> Result<()> {
    writer.write_all(&packet_type.to_be_bytes())?;
    writer.write_all(&(payload.len() as u32).to_be_bytes())?;
    writer.write_all(payload)?;
    writer.flush()?;
    Ok(())
}

pub fn read_packet<R: Read>(reader: &mut R) -> Result<Packet> {
    let mut header = [0u8; PACKET_HEADER_LEN];
    reader.read_exact(&mut header)?;

    let packet_type = u32::from_be_bytes(header[0..4].try_into().unwrap());
    let payload_len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
    if payload_len > MAX_PACKET_SIZE {
        return Err(Error::Protocol("packet too large"));
    }

    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;
    Ok(Packet::new(packet_type, payload))
}

pub fn roundtrip<S: Read + Write>(
    stream: &mut S,
    request_type: u32,
    payload: &[u8],
    expected_response_type: u32,
) -> Result<Packet> {
    write_packet(stream, request_type, payload)?;
    loop {
        let packet = read_packet(stream)?;
        if packet.packet_type == ANTOAN_NOP && packet.payload.is_empty() {
            continue;
        }
        if packet.packet_type != expected_response_type {
            return Err(Error::ProtocolDetail(format!(
                "unexpected response packet type: got {}, expected {}",
                packet.packet_type, expected_response_type
            )));
        }
        return Ok(packet);
    }
}

pub fn push_u8(buf: &mut Vec<u8>, value: u8) {
    buf.push(value);
}

pub fn push_u16_be(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub fn push_u32_be(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub fn push_u64_be(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

pub fn decode_u16_be(data: &[u8]) -> Result<u16> {
    if data.len() < 2 {
        return Err(Error::Protocol("u16 decode underflow"));
    }
    Ok(u16::from_be_bytes([data[0], data[1]]))
}

pub fn decode_u32_be(data: &[u8]) -> Result<u32> {
    if data.len() < 4 {
        return Err(Error::Protocol("u32 decode underflow"));
    }
    Ok(u32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

pub fn decode_u64_be(data: &[u8]) -> Result<u64> {
    if data.len() < 8 {
        return Err(Error::Protocol("u64 decode underflow"));
    }
    Ok(u64::from_be_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

pub fn io_error(kind: io::ErrorKind, message: &'static str) -> Error {
    Error::Io(io::Error::new(kind, message))
}

pub fn version_int(major: u16, mid: u8, minor: u8) -> u32 {
    ((major as u32) << 16) | ((mid as u32) << 8) | minor as u32
}

pub fn response_status(packet: &Packet) -> Option<u8> {
    if packet.payload.len() == 1 {
        Some(packet.payload[0])
    } else if packet.payload.len() == 5 {
        Some(packet.payload[4])
    } else {
        None
    }
}

pub fn expect_msgid(packet: &Packet, msgid: u32) -> Result<()> {
    if packet.payload.len() < 4 {
        return Err(Error::Protocol("short master reply"));
    }
    if decode_u32_be(&packet.payload[..4])? != msgid {
        return Err(Error::Protocol("stale or mismatched master reply"));
    }
    Ok(())
}

pub fn expect_simple_status(packet: &Packet, msgid: u32, op: &'static str) -> Result<()> {
    expect_msgid(packet, msgid)?;
    if packet.payload.len() < 5 {
        return Err(Error::Protocol("short status reply"));
    }
    let status = packet.payload[4];
    if status != 0 {
        return Err(Error::MoosefsStatus { op, status });
    }
    Ok(())
}

pub fn attr_type(attr: &[u8]) -> u8 {
    if attr.len() < 3 {
        return 0;
    }
    if attr[0] < 64 {
        attr[1] >> 4
    } else {
        attr[0] & 0x7f
    }
}

pub fn attr_size(attr: &[u8]) -> u64 {
    if attr.len() < 35 {
        return 0;
    }
    u64::from_be_bytes([
        attr[27], attr[28], attr[29], attr[30], attr[31], attr[32], attr[33], attr[34],
    ])
}

pub fn crc32_update(seed: u32, data: &[u8]) -> u32 {
    let mut crc = !seed;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg() & 0xEDB8_8320;
            crc = (crc >> 1) ^ mask;
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::{attr_size, attr_type, crc32_update, decode_u32_be, read_packet, write_packet, Packet};
    use std::io::Cursor;

    #[test]
    fn packet_roundtrip_is_big_endian() {
        let mut buf = Vec::new();
        write_packet(&mut buf, 0x0102_0304, &[0xaa, 0xbb]).unwrap();
        assert_eq!(&buf[..4], &[1, 2, 3, 4]);
        assert_eq!(decode_u32_be(&buf[4..8]).unwrap(), 2);
        let packet = read_packet(&mut Cursor::new(buf)).unwrap();
        assert_eq!(packet, Packet::new(0x0102_0304, vec![0xaa, 0xbb]));
    }

    #[test]
    fn crc32_matches_known_value() {
        assert_eq!(crc32_update(0, b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn attr_helpers_follow_qemu_layout() {
        let mut attr = [0u8; 35];
        attr[0] = 0;
        attr[1] = super::TYPE_DIRECTORY << 4;
        attr[27..35].copy_from_slice(&123u64.to_be_bytes());
        assert_eq!(attr_type(&attr), super::TYPE_DIRECTORY);
        assert_eq!(attr_size(&attr), 123);
    }
}
