use std::io::{Cursor, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
#[cfg(feature = "quic")]
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "quic")]
use quinn::crypto::rustls::QuicClientConfig;
#[cfg(feature = "quic")]
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream};
#[cfg(feature = "quic")]
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
#[cfg(feature = "quic")]
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
#[cfg(feature = "quic")]
use tokio::io::AsyncWriteExt;
#[cfg(feature = "quic")]
use tokio::runtime::{Builder, Runtime};

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
    pub insecure_skip_verify: bool,
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

#[cfg(feature = "quic")]
pub struct QuicStreamMaster {
    runtime: Runtime,
    endpoint: Endpoint,
    connection: Connection,
    send: SendStream,
    recv: RecvStream,
    read_cursor: Cursor<Vec<u8>>,
    pending_write: Vec<u8>,
}

impl Default for QuicConnectConfig {
    fn default() -> Self {
        Self {
            server_name: None,
            alpn: b"mfs-direct/exp1".to_vec(),
            zero_rtt: true,
            datagrams: false,
            insecure_skip_verify: true,
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

    pub fn with_insecure_skip_verify(mut self, enabled: bool) -> Self {
        self.insecure_skip_verify = enabled;
        self
    }
}

impl PacketModeMaster {
    pub fn connect(master_addr: &str, config: &QuicConnectConfig) -> Result<(Self, QuicEndpointInfo)> {
        let hello = probe_quic_endpoint(master_addr, config)?;
        if !hello.supports_datagram_mode() {
            return Err(Error::Unsupported(
                "QUIC endpoint does not advertise MooseFS datagram mode",
            ));
        }
        let udp_addr = resolve_quic_socket_addr(master_addr)?;
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

#[cfg(feature = "quic")]
impl QuicStreamMaster {
    pub fn connect(master_addr: &str, config: &QuicConnectConfig) -> Result<(Self, QuicEndpointInfo)> {
        let hello = probe_quic_endpoint(master_addr, config)?;
        if !hello.supports_tls_quic() {
            return Err(Error::Unsupported(
                "QUIC endpoint does not advertise native TLS-backed QUIC",
            ));
        }

        let udp_addr = resolve_quic_socket_addr(master_addr)?;
        let server_name = config
            .server_name
            .clone()
            .unwrap_or_else(|| master_host(master_addr).to_string());
        let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(Error::Io)?;
        let client_config = build_quic_client_config(config)?;
        let (endpoint, connection, send, recv) = runtime.block_on(async {
            let bind_addr: SocketAddr = if udp_addr.is_ipv6() {
                "[::]:0".parse().expect("literal IPv6 bind address is valid")
            } else {
                "0.0.0.0:0".parse().expect("literal IPv4 bind address is valid")
            };
            let mut endpoint = Endpoint::client(bind_addr)
                .map_err(|err| Error::ProtocolDetail(format!("QUIC endpoint bind failed: {err}")))?;
            endpoint.set_default_client_config(client_config);
            let connection = endpoint
                .connect(udp_addr, &server_name)
                .map_err(|err| Error::ProtocolDetail(format!("QUIC connect setup failed: {err}")))?
                .await
                .map_err(|err| Error::ProtocolDetail(format!("QUIC connect failed: {err}")))?;
            let (send, recv) = connection
                .open_bi()
                .await
                .map_err(|err| Error::ProtocolDetail(format!("QUIC control stream open failed: {err}")))?;
            Ok::<_, Error>((endpoint, connection, send, recv))
        })?;
        Ok((
            Self {
                runtime,
                endpoint,
                connection,
                send,
                recv,
                read_cursor: Cursor::new(Vec::new()),
                pending_write: Vec::new(),
            },
            hello,
        ))
    }

    pub fn endpoint_info(master_addr: &str, config: &QuicConnectConfig) -> Result<QuicEndpointInfo> {
        probe_quic_endpoint(master_addr, config)
    }

    fn send_pending(&mut self) -> Result<()> {
        if self.pending_write.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending_write);
        self.runtime.block_on(async {
            self.send
                .write_all(&pending)
                .await
                .map_err(|err| Error::ProtocolDetail(format!("QUIC stream write failed: {err}")))?;
            self.send
                .flush()
                .await
                .map_err(|err| Error::ProtocolDetail(format!("QUIC stream flush failed: {err}")))?;
            Ok::<_, Error>(())
        })
    }

    fn refill_read_cursor(&mut self) -> Result<()> {
        if (self.read_cursor.position() as usize) < self.read_cursor.get_ref().len() {
            return Ok(());
        }
        self.send_pending()?;
        let received = self.runtime.block_on(async {
            let mut buf = vec![0u8; 8192];
            let count = self
                .recv
                .read(&mut buf)
                .await
                .map_err(|err| Error::ProtocolDetail(format!("QUIC stream read failed: {err}")))?;
            let count = count.ok_or(Error::Protocol("QUIC control stream closed"))?;
            buf.truncate(count);
            Ok::<_, Error>(buf)
        })?;
        self.read_cursor = Cursor::new(received);
        Ok(())
    }
}

#[cfg(feature = "quic")]
impl Drop for QuicStreamMaster {
    fn drop(&mut self) {
        self.connection.close(0u32.into(), b"done");
        let _ = self.runtime.block_on(self.endpoint.wait_idle());
    }
}

impl Read for PacketModeMaster {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.ensure_response_buffer()
            .map_err(|err| std::io::Error::other(err.to_string()))?;
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

#[cfg(feature = "quic")]
impl Read for QuicStreamMaster {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while (self.read_cursor.position() as usize) >= self.read_cursor.get_ref().len() {
            self.refill_read_cursor()
                .map_err(|err| std::io::Error::other(err.to_string()))?;
        }
        self.read_cursor.read(buf)
    }
}

#[cfg(feature = "quic")]
impl Write for QuicStreamMaster {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.pending_write.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.send_pending()
            .map_err(|err| std::io::Error::other(err.to_string()))
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

#[cfg(feature = "quic")]
fn build_quic_client_config(config: &QuicConnectConfig) -> Result<ClientConfig> {
    let crypto = if config.insecure_skip_verify {
        let mut crypto = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(SkipServerVerification::new())
            .with_no_client_auth();
        crypto.alpn_protocols = vec![config.alpn.clone()];
        crypto.enable_early_data = config.zero_rtt;
        crypto
    } else {
        return Err(Error::Unsupported(
            "certificate-verified QUIC is not wired yet; use insecure_skip_verify for unstable",
        ));
    };
    let quic_crypto = QuicClientConfig::try_from(crypto)
        .map_err(|err| Error::ProtocolDetail(format!("QUIC TLS config failed: {err}")))?;
    Ok(ClientConfig::new(Arc::new(quic_crypto)))
}

fn resolve_quic_socket_addr(master_addr: &str) -> Result<SocketAddr> {
    let addr = derive_quic_addr(master_addr)?;
    addr.to_socket_addrs()?
        .next()
        .ok_or(Error::InvalidInput("master_addr did not resolve"))
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

#[cfg_attr(not(feature = "quic"), allow(dead_code))]
fn master_host(master_addr: &str) -> &str {
    master_addr.rsplit_once(':').map(|(host, _)| host).unwrap_or(master_addr)
}

#[cfg(feature = "quic")]
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

#[cfg(feature = "quic")]
impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

#[cfg(feature = "quic")]
impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::{QuicConnectConfig, derive_quic_addr, master_host};

    #[test]
    fn derives_quic_port_from_master_port() {
        assert_eq!(derive_quic_addr("10.7.1.195:19521").unwrap(), "10.7.1.195:19523");
        assert_eq!(derive_quic_addr("mfsmaster").unwrap(), "mfsmaster:9423");
    }

    #[test]
    fn default_quic_mode_is_explicitly_insecure_for_unstable() {
        let config = QuicConnectConfig::default();
        assert!(config.insecure_skip_verify);
    }

    #[test]
    fn host_extraction_reuses_master_hostname() {
        assert_eq!(master_host("127.0.0.1:19621"), "127.0.0.1");
        assert_eq!(master_host("mfsmaster"), "mfsmaster");
    }
}
