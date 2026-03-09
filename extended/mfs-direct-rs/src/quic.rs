use std::io::{Cursor, Read, Write};
use std::net::UdpSocket;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::protocol::{
    decode_u16_be, decode_u32_be, read_packet, version_int, write_packet, CLTOMA_QUIC_HELLO,
    MATOCL_QUIC_HELLO, QUIC_HELLO_FLAG_DATAGRAMS, QUIC_HELLO_FLAG_PACKET_MODE,
    QUIC_HELLO_FLAG_TLS, QUIC_HELLO_FLAG_ZERO_RTT, QUIC_HELLO_MAGIC, VERSION_MAJOR,
    VERSION_MID, VERSION_MINOR,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicConnectConfig {
    pub server_name: Option<String>,
    pub alpn: Vec<u8>,
    pub zero_rtt: bool,
    pub datagrams: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicEndpointInfo {
    pub server_version: u32,
    pub flags: u32,
    pub tcp_port: u16,
    pub udp_port: u16,
    pub max_datagram: u16,
    pub alpn: Vec<u8>,
}

impl QuicEndpointInfo {
    pub fn supports_datagram_mode(&self) -> bool {
        self.flags & QUIC_HELLO_FLAG_PACKET_MODE != 0
    }

    pub fn supports_tls_quic(&self) -> bool {
        self.flags & QUIC_HELLO_FLAG_TLS != 0
    }
}

pub struct PacketModeMaster {
    socket: UdpSocket,
    read_cursor: Cursor<Vec<u8>>,
    pending_write: Vec<u8>,
    next_reqid: u32,
}

impl Default for QuicConnectConfig {
    fn default() -> Self {
        Self {
            server_name: None,
            alpn: b"mfs-direct/exp1".to_vec(),
            zero_rtt: true,
            datagrams: false,
        }
    }
}

impl QuicConnectConfig {
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = Some(server_name.into());
        self
    }

    pub fn with_alpn(mut self, alpn: impl Into<Vec<u8>>) -> Self {
        self.alpn = alpn.into();
        self
    }

    pub fn with_zero_rtt(mut self, enabled: bool) -> Self {
        self.zero_rtt = enabled;
        self
    }

    pub fn with_datagrams(mut self, enabled: bool) -> Self {
        self.datagrams = enabled;
        self
    }
}

impl PacketModeMaster {
    pub fn connect(master_addr: &str, config: &QuicConnectConfig) -> Result<(Self, QuicEndpointInfo)> {
        let hello = probe_quic_endpoint(master_addr, config)?;
        if !hello.supports_datagram_mode() {
            return Err(Error::Unsupported("QUIC endpoint does not advertise MooseFS datagram mode"));
        }
        let udp_addr = derive_quic_addr(master_addr)?;
        let socket = UdpSocket::bind("0.0.0.0:0")?;
        socket.set_read_timeout(Some(Duration::from_secs(2)))?;
        socket.connect(&udp_addr)?;
        Ok((
            Self {
                socket,
                read_cursor: Cursor::new(Vec::new()),
                pending_write: Vec::new(),
                next_reqid: 1,
            },
            hello,
        ))
    }

    fn ensure_response_buffer(&mut self) -> Result<()> {
        if (self.read_cursor.position() as usize) < self.read_cursor.get_ref().len() {
            return Ok(());
        }
        if self.pending_write.is_empty() {
            return Ok(());
        }

        self.socket.send(&self.pending_write)?;

        let mut recv = vec![0u8; 2048];
        let received = self.socket.recv(&mut recv)?;
        self.pending_write.clear();
        self.next_reqid = self.next_reqid.wrapping_add(1).max(1);
        self.read_cursor = Cursor::new(recv[..received].to_vec());
        Ok(())
    }
}

impl Read for PacketModeMaster {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.ensure_response_buffer()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))?;
        self.read_cursor.read(buf)
    }
}

impl Write for PacketModeMaster {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pending_write.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn probe_quic_endpoint(master_addr: &str, config: &QuicConnectConfig) -> Result<QuicEndpointInfo> {
    let udp_addr = derive_quic_addr(master_addr)?;
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_read_timeout(Some(Duration::from_secs(1)))?;
    socket.connect(&udp_addr)?;

    let mut payload = Vec::with_capacity(13 + config.alpn.len());
    payload.extend_from_slice(&QUIC_HELLO_MAGIC.to_be_bytes());
    payload.extend_from_slice(&version_int(VERSION_MAJOR, VERSION_MID, VERSION_MINOR).to_be_bytes());
    let mut flags = 0u32;
    if config.zero_rtt {
        flags |= QUIC_HELLO_FLAG_ZERO_RTT;
    }
    if config.datagrams {
        flags |= QUIC_HELLO_FLAG_DATAGRAMS;
    }
    payload.extend_from_slice(&flags.to_be_bytes());
    payload.push(config.alpn.len() as u8);
    payload.extend_from_slice(&config.alpn);

    let mut request = Vec::new();
    write_packet(&mut request, CLTOMA_QUIC_HELLO, &payload)?;
    socket.send(&request)?;

    let mut recv = vec![0u8; 2048];
    let received = socket.recv(&mut recv)?;
    let packet = read_packet(&mut Cursor::new(&recv[..received]))?;
    if packet.packet_type != MATOCL_QUIC_HELLO {
        return Err(Error::ProtocolDetail(format!(
            "unexpected QUIC hello response type {}",
            packet.packet_type
        )));
    }
    if packet.payload.len() < 16 {
        return Err(Error::Protocol("short QUIC hello response"));
    }
    let status = packet.payload[0];
    if status != 0 {
        return Err(Error::MoosefsStatus {
            op: "quic_hello",
            status,
        });
    }
    let server_version = decode_u32_be(&packet.payload[1..5])?;
    let server_flags = decode_u32_be(&packet.payload[5..9])?;
    let tcp_port = decode_u16_be(&packet.payload[9..11])?;
    let udp_port = decode_u16_be(&packet.payload[11..13])?;
    let max_datagram = decode_u16_be(&packet.payload[13..15])?;
    let alpn_len = packet.payload[15] as usize;
    if packet.payload.len() != 16 + alpn_len {
        return Err(Error::Protocol("malformed QUIC hello response"));
    }
    let alpn = packet.payload[16..].to_vec();
    Ok(QuicEndpointInfo {
        server_version,
        flags: server_flags,
        tcp_port,
        udp_port,
        max_datagram,
        alpn,
    })
}

fn derive_quic_addr(master_addr: &str) -> Result<String> {
    if master_addr.is_empty() {
        return Err(Error::InvalidInput("master_addr must not be empty"));
    }
    if let Some((host, port)) = master_addr.rsplit_once(':') {
        let port = port
            .parse::<u16>()
            .map_err(|_| Error::InvalidInput("master_addr port must be numeric"))?;
        return Ok(format!("{host}:{}", port.saturating_add(2)));
    }
    Ok(format!("{master_addr}:9423"))
}

#[cfg(test)]
mod tests {
    use super::derive_quic_addr;

    #[test]
    fn derives_quic_port_from_master_port() {
        assert_eq!(derive_quic_addr("10.7.1.195:19521").unwrap(), "10.7.1.195:19523");
        assert_eq!(derive_quic_addr("mfsmaster").unwrap(), "mfsmaster:9423");
    }
}
