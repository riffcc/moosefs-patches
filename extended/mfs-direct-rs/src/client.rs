use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;

use crate::error::{Error, Result};
use crate::protocol::{
    attr_size, attr_type, crc32_update, decode_u16_be, decode_u32_be, decode_u64_be,
    expect_msgid, expect_simple_status, read_packet, response_status, roundtrip, write_packet,
    version_int, BLOCK_SIZE, CHUNKOPFLAG_CANMODTIME, CHUNK_SIZE, CLTOMA_FUSE_CREATE,
    CLTOMA_FUSE_GETATTR, CLTOMA_FUSE_LOOKUP, CLTOMA_FUSE_MKDIR, CLTOMA_FUSE_OPEN,
    CLTOMA_FUSE_READ_CHUNK, CLTOMA_FUSE_REGISTER, CLTOMA_FUSE_RENAME,
    CLTOMA_FUSE_RMDIR, CLTOMA_FUSE_TRUNCATE, CLTOMA_FUSE_UNLINK, CLTOMA_FUSE_WRITE_CHUNK,
    CLTOMA_FUSE_WRITE_CHUNK_END, CLTOCS_READ, CLTOCS_WRITE, CLTOCS_WRITE_DATA,
    CLTOCS_WRITE_FINISH, CSTOCL_READ_DATA, CSTOCL_READ_STATUS, CSTOCL_WRITE_STATUS,
    DEFAULT_MASTER_PORT, MATOCL_FUSE_CREATE, MATOCL_FUSE_GETATTR, MATOCL_FUSE_LOOKUP,
    MATOCL_FUSE_MKDIR, MATOCL_FUSE_OPEN, MATOCL_FUSE_READ_CHUNK, MATOCL_FUSE_REGISTER,
    MATOCL_FUSE_RENAME, MATOCL_FUSE_RMDIR, MATOCL_FUSE_TRUNCATE, MATOCL_FUSE_UNLINK,
    MATOCL_FUSE_WRITE_CHUNK, MATOCL_FUSE_WRITE_CHUNK_END, OPEN_READWRITE, REGISTER_BLOB_ACL,
    REGISTER_NEWSESSION, REGISTER_RECONNECT, ROOT_INODE, TYPE_DIRECTORY, TYPE_FILE,
    VERSION_MAJOR, VERSION_MID, VERSION_MINOR,
};
use crate::quic::{probe_quic_endpoint, PacketModeMaster, QuicConnectConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectOptions {
    pub client_id: String,
    pub subdir: String,
    pub password_md5: Option<[u8; 16]>,
    pub max_in_flight_write_fragments: usize,
    pub experimental_ooo_write_acks: bool,
    pub transport: TransportKind,
    pub quic: QuicConnectConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    QuicExperimental,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            client_id: "moosefs-direct".to_string(),
            subdir: "/".to_string(),
            password_md5: None,
            max_in_flight_write_fragments: 16,
            experimental_ooo_write_acks: false,
            transport: TransportKind::Tcp,
            quic: QuicConnectConfig::default(),
        }
    }
}

impl ConnectOptions {
    pub fn with_password(mut self, password: &str) -> Self {
        self.password_md5 = Some(md5::compute(password.as_bytes()).0);
        self
    }

    pub fn with_max_in_flight_write_fragments(mut self, count: usize) -> Self {
        self.max_in_flight_write_fragments = count.max(1);
        self
    }

    pub fn with_experimental_ooo_write_acks(mut self, enabled: bool) -> Self {
        self.experimental_ooo_write_acks = enabled;
        self
    }

    pub fn with_transport(mut self, transport: TransportKind) -> Self {
        self.transport = transport;
        self
    }

    pub fn with_quic_config(mut self, quic: QuicConnectConfig) -> Self {
        self.quic = quic;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterSession {
    pub session_id: u32,
    pub master_version: u32,
    pub next_msgid: u32,
}

impl MasterSession {
    pub fn new(session_id: u32, master_version: u32) -> Self {
        Self {
            session_id,
            master_version,
            next_msgid: 1,
        }
    }

    pub fn next_msgid(&mut self) -> u32 {
        let current = self.next_msgid;
        self.next_msgid = self.next_msgid.wrapping_add(1);
        if self.next_msgid == 0 {
            self.next_msgid = 1;
        }
        current
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkReplica {
    pub host: String,
    pub port: u16,
    pub chunkserver_version: u32,
    pub label_mask: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkLocation {
    pub protocol_version: u8,
    pub file_length: u64,
    pub chunk_id: u64,
    pub chunk_version: u32,
    pub chunk_index: u32,
    pub replicas: Vec<ChunkReplica>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePlan {
    pub path: String,
    pub inode: u64,
    pub file_size: u64,
    pub chunk_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenFile {
    pub path: String,
    pub inode: u32,
    pub size: u64,
}

pub struct Client<S = TcpStream> {
    master: S,
    session: Option<MasterSession>,
    max_in_flight_write_fragments: usize,
    experimental_ooo_write_acks: bool,
    master_addr: Option<String>,
    connect_options: Option<ConnectOptions>,
    read_chunk_cache: BTreeMap<(u32, u32), ChunkLocation>,
    write_chunk_cache: BTreeMap<(u32, u32), ChunkLocation>,
    read_session: Option<ChunkReadSession>,
}

struct ChunkReadSession {
    host: String,
    port: u16,
    protocol_id: Option<u8>,
    stream: TcpStream,
}

struct ChunkWriteSession {
    host: String,
    port: u16,
    protocol_id: Option<u8>,
    stream: TcpStream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u32,
    end: u32,
}

impl ByteRange {
    fn new(start: u32, end: u32) -> Self {
        Self { start, end }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ConfirmedRanges {
    ranges: Vec<ByteRange>,
}

impl ConfirmedRanges {
    fn insert(&mut self, mut incoming: ByteRange) {
        if incoming.start >= incoming.end {
            return;
        }

        let mut merged = Vec::with_capacity(self.ranges.len() + 1);
        let mut inserted = false;
        for range in self.ranges.drain(..) {
            if range.end < incoming.start {
                merged.push(range);
            } else if incoming.end < range.start {
                if !inserted {
                    merged.push(incoming);
                    inserted = true;
                }
                merged.push(range);
            } else {
                incoming.start = incoming.start.min(range.start);
                incoming.end = incoming.end.max(range.end);
            }
        }
        if !inserted {
            merged.push(incoming);
        }
        self.ranges = merged;
    }

    fn covers(&self, required: ByteRange) -> bool {
        self.ranges
            .iter()
            .any(|range| range.start <= required.start && range.end >= required.end)
    }
}

impl Client<TcpStream> {
    pub fn connect(master_addr: &str, options: ConnectOptions) -> Result<Self> {
        if master_addr.is_empty() {
            return Err(Error::InvalidInput("master_addr must not be empty"));
        }
        if options.subdir.is_empty() || !options.subdir.starts_with('/') {
            return Err(Error::InvalidInput("subdir must be absolute"));
        }
        if options.transport == TransportKind::QuicExperimental {
            let hello = probe_quic_endpoint(master_addr, &options.quic)?;
            return Err(Error::ProtocolDetail(format!(
                "experimental QUIC bootstrap succeeded (server_version=0x{server_version:06x}, flags=0x{flags:08x}, tcp_port={tcp_port}, udp_port={udp_port}, max_datagram={max_datagram}, alpn={alpn}), but full QUIC stream transport is not implemented yet",
                server_version = hello.server_version,
                flags = hello.flags,
                tcp_port = hello.tcp_port,
                udp_port = hello.udp_port,
                max_datagram = hello.max_datagram,
                alpn = String::from_utf8_lossy(&hello.alpn),
            )));
        }

        let endpoint = normalize_master_addr(master_addr);
        let master = TcpStream::connect(&endpoint)?;
        let _ = master.set_nodelay(true);
        Ok(Self {
            master,
            session: None,
            max_in_flight_write_fragments: options.max_in_flight_write_fragments.max(1),
            experimental_ooo_write_acks: options.experimental_ooo_write_acks,
            master_addr: Some(endpoint),
            connect_options: Some(options),
            read_chunk_cache: BTreeMap::new(),
            write_chunk_cache: BTreeMap::new(),
            read_session: None,
        })
    }

    pub fn connect_registered(master_addr: &str, options: ConnectOptions) -> Result<Self> {
        let mut client = Self::connect(master_addr, options.clone())?;
        client.register_session(&options)?;
        Ok(client)
    }

    fn spawn_prefetch_client(&self) -> Result<Self> {
        let master_addr = self
            .master_addr
            .as_deref()
            .ok_or(Error::Unsupported("prefetch requires a TcpStream-backed client"))?;
        let options = self
            .connect_options
            .clone()
            .ok_or(Error::Unsupported("prefetch requires connect options"))?;
        let session = self
            .session
            .as_ref()
            .ok_or(Error::Protocol("session not registered"))?;
        let mut client = Self::connect(master_addr, options)?;
        client.reconnect_session(session.session_id, session.master_version)?;
        Ok(client)
    }

    pub fn read_all(&mut self, path: &str) -> Result<Vec<u8>> {
        let file = self.open_file(path)?;
        let file_size = file.size;
        if file_size == 0 {
            return Ok(Vec::new());
        }

        let mut out = vec![0u8; file_size as usize];
        self.read_at(&file, 0, &mut out)?;
        Ok(out)
    }

    pub fn write_all(&mut self, path: &str, bytes: &[u8]) -> Result<()> {
        let mut cursor = std::io::Cursor::new(bytes);
        self.write_from_reader(path, bytes.len() as u64, &mut cursor)
    }

    pub fn write_from_reader<R: Read>(&mut self, path: &str, size: u64, reader: &mut R) -> Result<()> {
        validate_absolute_path(path)?;
        let inode = self.ensure_file(path, size)?;
        if size == 0 {
            return Ok(());
        }

        let session = self
            .session
            .as_ref()
            .ok_or(Error::Protocol("session not registered"))?;
        let master_version = session.master_version;
        let mut written = 0u64;
        let mut chunk_session = None;
        let mut prefetched_chunk = None;
        let mut prefetch_client = self.spawn_prefetch_client().ok();

        while written < size {
            let file_offset = written;
            let chunk_index = (file_offset / CHUNK_SIZE) as u32;
            let chunk_offset = (file_offset % CHUNK_SIZE) as u32;
            let remaining = (size - written) as usize;
            let to_write = remaining.min((CHUNK_SIZE as usize) - (chunk_offset as usize));
            let mut chunk_buf = vec![0u8; to_write];
            reader.read_exact(&mut chunk_buf)?;

            let chunk_location = match prefetched_chunk.take() {
                Some(chunk) => chunk,
                None => self.master_write_chunk(inode as u32, chunk_index)?,
            };
            let has_next_chunk = written + (to_write as u64) < size;
            if has_next_chunk {
                let next_chunk_index = chunk_index + 1;
                if let Some(prefetch) = prefetch_client.as_mut() {
                    prefetched_chunk = Some(std::thread::scope(|scope| -> Result<ChunkLocation> {
                        let handle =
                            scope.spawn(|| prefetch.master_write_chunk(inode as u32, next_chunk_index));
                        self.chunkserver_write(
                            &chunk_location,
                            chunk_offset,
                            &chunk_buf,
                            &mut chunk_session,
                        )?;
                        match handle.join() {
                            Ok(result) => result,
                            Err(_) => Err(Error::Protocol("prefetch worker panicked")),
                        }
                    })?);
                } else {
                    self.chunkserver_write(
                        &chunk_location,
                        chunk_offset,
                        &chunk_buf,
                        &mut chunk_session,
                    )?;
                    prefetched_chunk = Some(self.master_write_chunk(inode as u32, next_chunk_index)?);
                }
            } else {
                self.chunkserver_write(
                    &chunk_location,
                    chunk_offset,
                    &chunk_buf,
                    &mut chunk_session,
                )?;
            }

            self.master_write_chunk_end(
                master_version,
                inode as u32,
                chunk_index,
                chunk_location.chunk_id,
                size,
                chunk_offset,
                to_write as u32,
            )?;

            written += to_write as u64;
        }

        Ok(())
    }

    pub fn open_file(&mut self, path: &str) -> Result<OpenFile> {
        let inode = self.lookup_path(path)?;
        let (_, file_size, file_type) = self.getattr(inode as u32)?;
        if file_type != TYPE_FILE {
            return Err(Error::Protocol("path does not resolve to a regular file"));
        }
        Ok(OpenFile {
            path: path.to_string(),
            inode: inode as u32,
            size: file_size,
        })
    }

    pub fn ensure_file_len(&mut self, path: &str, size: u64) -> Result<OpenFile> {
        let inode = self.ensure_file(path, size)? as u32;
        self.read_chunk_cache.retain(|(cached_inode, _), _| *cached_inode != inode);
        self.write_chunk_cache
            .retain(|(cached_inode, _), _| *cached_inode != inode);
        Ok(OpenFile {
            path: path.to_string(),
            inode,
            size,
        })
    }

    pub fn read_at(&mut self, file: &OpenFile, offset: u64, out: &mut [u8]) -> Result<()> {
        let end = offset
            .checked_add(out.len() as u64)
            .ok_or(Error::InvalidInput("read range overflows"))?;
        if end > file.size {
            return Err(Error::InvalidInput("read range exceeds file size"));
        }

        let mut filled = 0usize;
        while filled < out.len() {
            let file_offset = offset + filled as u64;
            let chunk_index = (file_offset / CHUNK_SIZE) as u32;
            let chunk_offset = (file_offset % CHUNK_SIZE) as u32;
            let remaining = out.len() - filled;
            let to_read = remaining.min((CHUNK_SIZE as usize) - chunk_offset as usize);
            let chunk = self.master_read_chunk_cached(file.inode, chunk_index)?;
            self.chunkserver_read(&chunk, chunk_offset, &mut out[filled..filled + to_read])?;
            filled += to_read;
        }

        Ok(())
    }

    pub fn write_at(&mut self, file: &OpenFile, offset: u64, bytes: &[u8]) -> Result<()> {
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::InvalidInput("write range overflows"))?;
        if end > file.size {
            return Err(Error::InvalidInput("write range exceeds file size"));
        }

        let session = self
            .session
            .as_ref()
            .ok_or(Error::Protocol("session not registered"))?;
        let master_version = session.master_version;
        let mut written = 0usize;
        let mut chunk_session = None;

        while written < bytes.len() {
            let file_offset = offset + written as u64;
            let chunk_index = (file_offset / CHUNK_SIZE) as u32;
            let chunk_offset = (file_offset % CHUNK_SIZE) as u32;
            let remaining = bytes.len() - written;
            let to_write = remaining.min((CHUNK_SIZE as usize) - chunk_offset as usize);
            self.read_chunk_cache.remove(&(file.inode, chunk_index));
            let chunk_location = self.master_write_chunk_cached(file.inode, chunk_index)?;
            self.chunkserver_write(
                &chunk_location,
                chunk_offset,
                &bytes[written..written + to_write],
                &mut chunk_session,
            )?;
            self.master_write_chunk_end(
                master_version,
                file.inode,
                chunk_index,
                chunk_location.chunk_id,
                file.size,
                chunk_offset,
                to_write as u32,
            )?;
            written += to_write;
        }

        Ok(())
    }

    fn master_read_chunk_cached(&mut self, inode: u32, chunk_index: u32) -> Result<ChunkLocation> {
        if let Some(chunk) = self.read_chunk_cache.get(&(inode, chunk_index)) {
            return Ok(chunk.clone());
        }
        let chunk = self.master_read_chunk(inode, chunk_index)?;
        self.read_chunk_cache
            .insert((inode, chunk_index), chunk.clone());
        Ok(chunk)
    }

    fn master_write_chunk_cached(&mut self, inode: u32, chunk_index: u32) -> Result<ChunkLocation> {
        if let Some(chunk) = self.write_chunk_cache.get(&(inode, chunk_index)) {
            return Ok(chunk.clone());
        }
        let chunk = self.master_write_chunk(inode, chunk_index)?;
        self.write_chunk_cache
            .insert((inode, chunk_index), chunk.clone());
        Ok(chunk)
    }

}

impl Client<PacketModeMaster> {
    pub fn connect(master_addr: &str, options: ConnectOptions) -> Result<Self> {
        if master_addr.is_empty() {
            return Err(Error::InvalidInput("master_addr must not be empty"));
        }
        if options.subdir.is_empty() || !options.subdir.starts_with('/') {
            return Err(Error::InvalidInput("subdir must be absolute"));
        }
        let (master, _) = PacketModeMaster::connect(master_addr, &options.quic)?;
        Ok(Self {
            master,
            session: None,
            max_in_flight_write_fragments: options.max_in_flight_write_fragments.max(1),
            experimental_ooo_write_acks: options.experimental_ooo_write_acks,
            master_addr: Some(normalize_master_addr(master_addr)),
            connect_options: Some(options),
            read_chunk_cache: BTreeMap::new(),
            write_chunk_cache: BTreeMap::new(),
            read_session: None,
        })
    }

    pub fn connect_registered(master_addr: &str, options: ConnectOptions) -> Result<Self> {
        let mut client = Self::connect(master_addr, options.clone())?;
        client.register_session(&options)?;
        Ok(client)
    }
}

impl<S: Read + Write> Client<S> {
    pub fn new(master: S) -> Self {
        Self {
            master,
            session: None,
            max_in_flight_write_fragments: 16,
            experimental_ooo_write_acks: false,
            master_addr: None,
            connect_options: None,
            read_chunk_cache: BTreeMap::new(),
            write_chunk_cache: BTreeMap::new(),
            read_session: None,
        }
    }

    pub fn session(&self) -> Option<&MasterSession> {
        self.session.as_ref()
    }

    pub fn register_session(&mut self, options: &ConnectOptions) -> Result<&MasterSession> {
        if !options.subdir.starts_with('/') {
            return Err(Error::InvalidInput("subdir must be absolute"));
        }

        let client_id = if options.client_id.is_empty() {
            "moosefs-direct"
        } else {
            options.client_id.as_str()
        };
        let subdir = if options.subdir.is_empty() {
            "/"
        } else {
            options.subdir.as_str()
        };
        let password_digest = if let Some(password_md5) = options.password_md5 {
            let challenge = roundtrip(
                &mut self.master,
                CLTOMA_FUSE_REGISTER,
                &[REGISTER_BLOB_ACL.as_slice(), &[1]].concat(),
                MATOCL_FUSE_REGISTER,
            )?;
            if challenge.payload.len() != 32 {
                return Err(Error::Protocol("short password challenge"));
            }
            let mut digest_input = Vec::with_capacity(48);
            digest_input.extend_from_slice(&challenge.payload[..16]);
            digest_input.extend_from_slice(&password_md5);
            digest_input.extend_from_slice(&challenge.payload[16..32]);
            Some(md5::compute(digest_input).0)
        } else {
            None
        };

        let mut payload = Vec::new();
        payload.extend_from_slice(REGISTER_BLOB_ACL);
        payload.push(REGISTER_NEWSESSION);
        payload.extend_from_slice(&VERSION_MAJOR.to_be_bytes());
        payload.push(VERSION_MID);
        payload.push(VERSION_MINOR);
        payload.extend_from_slice(&((client_id.len() + 1) as u32).to_be_bytes());
        payload.extend_from_slice(client_id.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&((subdir.len() + 1) as u32).to_be_bytes());
        payload.extend_from_slice(subdir.as_bytes());
        payload.push(0);
        if let Some(digest) = password_digest {
            payload.extend_from_slice(&digest);
        }

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_REGISTER,
            &payload,
            MATOCL_FUSE_REGISTER,
        )?;

        if let Some(status) = response_status(&packet) {
            if packet.payload.len() == 1 && status != 0 {
                return Err(Error::MoosefsStatus {
                    op: "register",
                    status,
                });
            }
        }
        if packet.payload.len() < 8 {
            return Err(Error::Protocol("short register reply"));
        }

        let v1 = decode_u32_be(&packet.payload[0..4])?;
        let v2 = decode_u32_be(&packet.payload[4..8])?;
        let v1_is_version = (0x0001_0000..=0x0fff_ffff).contains(&v1);
        let session = MasterSession::new(
            if v1_is_version { v2 } else { v1 },
            if v1_is_version { v1 } else { v2 },
        );
        self.session = Some(session);
        Ok(self.session.as_ref().unwrap())
    }

    pub fn reconnect_session(
        &mut self,
        session_id: u32,
        master_version: u32,
    ) -> Result<&MasterSession> {
        let mut payload = Vec::with_capacity(73);
        payload.extend_from_slice(REGISTER_BLOB_ACL);
        payload.push(REGISTER_RECONNECT);
        payload.extend_from_slice(&session_id.to_be_bytes());
        payload.extend_from_slice(&master_version.to_be_bytes());

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_REGISTER,
            &payload,
            MATOCL_FUSE_REGISTER,
        )?;
        let status = response_status(&packet).ok_or(Error::Protocol("short reconnect reply"))?;
        if status != 0 {
            return Err(Error::MoosefsStatus {
                op: "reconnect",
                status,
            });
        }

        let negotiated_version = if packet.payload.len() >= 5 {
            decode_u32_be(&packet.payload[..4])?
        } else {
            master_version
        };
        self.session = Some(MasterSession::new(session_id, negotiated_version));
        Ok(self.session.as_ref().unwrap())
    }

    pub fn lookup_path(&mut self, path: &str) -> Result<u64> {
        let (inode, _, _) = self.lookup_path_details(path)?;
        Ok(inode)
    }

    pub fn stat_path(&mut self, path: &str) -> Result<Option<(u64, u64, u8)>> {
        match self.lookup_path_details(path) {
            Ok(details) => Ok(Some(details)),
            Err(Error::MoosefsStatus { status: 3, .. }) => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn path_exists(&mut self, path: &str) -> Result<bool> {
        Ok(self.stat_path(path)?.is_some())
    }

    pub fn ensure_dir_all(&mut self, path: &str) -> Result<u64> {
        validate_absolute_path(path)?;
        if path == "/" {
            return Ok(ROOT_INODE as u64);
        }

        let mut current_inode = ROOT_INODE as u64;
        let mut current_type = TYPE_DIRECTORY;
        let mut current_path = String::new();

        for component in path.split('/').filter(|part| !part.is_empty()) {
            current_path.push('/');
            current_path.push_str(component);

            match self.simple_lookup(current_inode as u32, component) {
                Ok((inode, attr)) => {
                    current_inode = inode;
                    current_type = attr_type(&attr);
                    if current_type != TYPE_DIRECTORY {
                        return Err(Error::ProtocolDetail(format!(
                            "path component {current_path} is not a directory"
                        )));
                    }
                }
                Err(Error::MoosefsStatus { status: 3, .. }) => {
                    current_inode = self.mkdir(current_inode as u32, component)? as u64;
                    current_type = TYPE_DIRECTORY;
                }
                Err(err) => return Err(err),
            }
        }

        debug_assert_eq!(current_type, TYPE_DIRECTORY);
        Ok(current_inode)
    }

    pub fn unlink_path(&mut self, path: &str) -> Result<()> {
        let (parent, leaf) = split_parent_and_name(path)?;
        self.unlink_name(parent, leaf, false)
    }

    pub fn remove_dir_path(&mut self, path: &str) -> Result<()> {
        let (parent, leaf) = split_parent_and_name(path)?;
        self.unlink_name(parent, leaf, true)
    }

    pub fn rename_path(&mut self, src: &str, dst: &str) -> Result<()> {
        let (src_parent, src_leaf) = split_parent_and_name(src)?;
        let (dst_parent, dst_leaf) = split_parent_and_name(dst)?;
        let (src_parent_inode, _, src_parent_type) = self.lookup_path_details(src_parent)?;
        if src_parent_type != TYPE_DIRECTORY {
            return Err(Error::Protocol("source parent path is not a directory"));
        }
        let dst_parent_inode = self.ensure_dir_all(dst_parent)?;
        self.rename_name(src_parent_inode as u32, src_leaf, dst_parent_inode as u32, dst_leaf)
    }

    pub fn ensure_file(&mut self, path: &str, size: u64) -> Result<u64> {
        validate_absolute_path(path)?;
        if path == "/" {
            return Err(Error::InvalidInput("path must name a file"));
        }

        let inode = match self.lookup_path(path) {
            Ok(inode) => inode,
            Err(Error::MoosefsStatus {
                status: 3, // ENOENT
                ..
            }) => {
                let (parent, _) = split_parent_and_name(path)?;
                self.ensure_dir_all(parent)?;
                self.create_file(path)?
            }
            Err(err) => return Err(err),
        };

        let (_, _, file_type) = self.getattr(inode as u32)?;
        if file_type != TYPE_FILE {
            return Err(Error::Protocol("path does not resolve to a regular file"));
        }

        self.open_check(inode as u32, OPEN_READWRITE)?;
        self.truncate(inode as u32, size)?;
        Ok(inode)
    }

    pub fn plan_write_all(&mut self, path: &str, bytes: &[u8]) -> Result<WritePlan> {
        validate_absolute_path(path)?;
        let inode = self.lookup_path(path)?;
        let chunk_count = if bytes.is_empty() {
            0
        } else {
            bytes.len().div_ceil(CHUNK_SIZE as usize)
        };
        Ok(WritePlan {
            path: path.to_string(),
            inode,
            file_size: bytes.len() as u64,
            chunk_count,
        })
    }

    pub fn debug_write_chunk_location(&mut self, path: &str) -> Result<ChunkLocation> {
        let inode = self.ensure_file(path, 0)?;
        self.master_write_chunk(inode as u32, 0)
    }

    pub fn debug_getattr(&mut self, path: &str) -> Result<(u64, u64, u8)> {
        let inode = self.lookup_path(path)?;
        self.getattr(inode as u32)
    }

    pub fn debug_open_check(&mut self, path: &str) -> Result<u64> {
        let inode = self.lookup_path(path)?;
        self.open_check(inode as u32, OPEN_READWRITE)?;
        Ok(inode)
    }

    pub fn debug_truncate_zero(&mut self, path: &str) -> Result<u64> {
        let inode = self.lookup_path(path)?;
        self.truncate(inode as u32, 0)?;
        Ok(inode)
    }

    pub fn debug_probe_write_setup(&mut self, path: &str) -> Result<Vec<String>> {
        let mut steps = Vec::new();
        let inode = self.lookup_path(path)?;
        steps.push(format!("lookup inode={inode}"));
        let (_, size, file_type) = self.getattr(inode as u32)?;
        steps.push(format!("getattr size={size} type={file_type}"));
        self.open_check(inode as u32, OPEN_READWRITE)?;
        steps.push("open ok".to_string());
        self.truncate(inode as u32, 0)?;
        steps.push("truncate ok".to_string());
        Ok(steps)
    }

    pub fn into_inner(self) -> S {
        self.master
    }

    fn session_mut(&mut self) -> Result<&mut MasterSession> {
        self.session
            .as_mut()
            .ok_or(Error::Protocol("session not registered"))
    }

    fn lookup_path_details(&mut self, path: &str) -> Result<(u64, u64, u8)> {
        validate_absolute_path(path)?;
        if path == "/" {
            return Ok((ROOT_INODE as u64, 0, TYPE_DIRECTORY));
        }

        let mut current_inode = ROOT_INODE as u64;
        let mut current_size = 0;
        let mut current_type = TYPE_DIRECTORY;

        for component in path.split('/').filter(|part| !part.is_empty()) {
            let (inode, attr) = self.simple_lookup(current_inode as u32, component)?;
            current_inode = inode;
            current_type = attr_type(&attr);
            current_size = attr_size(&attr);
            if component != path.rsplit('/').find(|part| !part.is_empty()).unwrap_or(component)
                && current_type != TYPE_DIRECTORY
            {
                return Err(Error::Protocol("intermediate path component is not a directory"));
            }
        }

        Ok((current_inode, current_size, current_type))
    }

    fn simple_lookup(&mut self, parent_inode: u32, name: &str) -> Result<(u64, Vec<u8>)> {
        if name.is_empty() || name.len() > u8::MAX as usize {
            return Err(Error::InvalidInput("invalid path component"));
        }

        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(17 + name.len());
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&parent_inode.to_be_bytes());
        payload.push(name.len() as u8);
        payload.extend_from_slice(name.as_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_LOOKUP,
            &payload,
            MATOCL_FUSE_LOOKUP,
        )?;
        expect_msgid(&packet, msgid)?;

        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: "lookup",
                status: packet.payload[4],
            });
        }
        if packet.payload.len() < 8 + 35 {
            return Err(Error::Protocol("short lookup reply"));
        }

        let inode = decode_u32_be(&packet.payload[4..8])? as u64;
        Ok((inode, packet.payload[8..].to_vec()))
    }

    fn getattr(&mut self, inode: u32) -> Result<(u64, u64, u8)> {
        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(17);
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&inode.to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_GETATTR,
            &payload,
            MATOCL_FUSE_GETATTR,
        )?;
        expect_msgid(&packet, msgid)?;
        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: "getattr",
                status: packet.payload[4],
            });
        }
        if packet.payload.len() < 4 + 35 {
            return Err(Error::Protocol("short getattr reply"));
        }

        let attr = &packet.payload[4..];
        Ok((inode as u64, attr_size(attr), attr_type(attr)))
    }

    fn create_file(&mut self, path: &str) -> Result<u64> {
        let (parent, leaf) = split_parent_and_name(path)?;
        let (parent_inode, _, parent_type) = self.lookup_path_details(parent)?;
        if parent_type != TYPE_DIRECTORY {
            return Err(Error::Protocol("parent path is not a directory"));
        }

        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(21 + leaf.len());
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&(parent_inode as u32).to_be_bytes());
        payload.push(leaf.len() as u8);
        payload.extend_from_slice(leaf.as_bytes());
        payload.extend_from_slice(&0o644u16.to_be_bytes());
        payload.extend_from_slice(&0u16.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_CREATE,
            &payload,
            MATOCL_FUSE_CREATE,
        )?;
        expect_msgid(&packet, msgid)?;
        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: "create",
                status: packet.payload[4],
            });
        }

        if packet.payload.len() == 4 + 35 || packet.payload.len() == 4 + 36 {
            return Ok(decode_u32_be(&packet.payload[4..8])? as u64);
        }
        if packet.payload.len() >= 5 + 35 {
            return Ok(decode_u32_be(&packet.payload[5..9])? as u64);
        }
        Err(Error::Protocol("short create reply"))
    }

    fn mkdir(&mut self, parent_inode: u32, leaf: &str) -> Result<u32> {
        if leaf.is_empty() || leaf.len() > u8::MAX as usize {
            return Err(Error::InvalidInput("invalid path component"));
        }

        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(22 + leaf.len());
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&parent_inode.to_be_bytes());
        payload.push(leaf.len() as u8);
        payload.extend_from_slice(leaf.as_bytes());
        payload.extend_from_slice(&0o755u16.to_be_bytes());
        payload.extend_from_slice(&0u16.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.push(0);

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_MKDIR,
            &payload,
            MATOCL_FUSE_MKDIR,
        )?;
        expect_msgid(&packet, msgid)?;
        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: "mkdir",
                status: packet.payload[4],
            });
        }
        if packet.payload.len() < 8 + 35 {
            return Err(Error::Protocol("short mkdir reply"));
        }
        Ok(decode_u32_be(&packet.payload[4..8])?)
    }

    fn unlink_name(&mut self, parent: &str, leaf: &str, is_dir: bool) -> Result<()> {
        let (parent_inode, _, parent_type) = self.lookup_path_details(parent)?;
        if parent_type != TYPE_DIRECTORY {
            return Err(Error::Protocol("parent path is not a directory"));
        }
        if leaf.is_empty() || leaf.len() > u8::MAX as usize {
            return Err(Error::InvalidInput("invalid path component"));
        }

        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(17 + leaf.len());
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&(parent_inode as u32).to_be_bytes());
        payload.push(leaf.len() as u8);
        payload.extend_from_slice(leaf.as_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());

        let packet = roundtrip(
            &mut self.master,
            if is_dir {
                CLTOMA_FUSE_RMDIR
            } else {
                CLTOMA_FUSE_UNLINK
            },
            &payload,
            if is_dir {
                MATOCL_FUSE_RMDIR
            } else {
                MATOCL_FUSE_UNLINK
            },
        )?;
        expect_msgid(&packet, msgid)?;
        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: if is_dir { "rmdir" } else { "unlink" },
                status: packet.payload[4],
            });
        }
        if packet.payload.len() != 8 {
            return Err(Error::Protocol("short unlink/rmdir reply"));
        }
        Ok(())
    }

    fn rename_name(
        &mut self,
        src_parent_inode: u32,
        src_leaf: &str,
        dst_parent_inode: u32,
        dst_leaf: &str,
    ) -> Result<()> {
        if src_leaf.is_empty()
            || src_leaf.len() > u8::MAX as usize
            || dst_leaf.is_empty()
            || dst_leaf.len() > u8::MAX as usize
        {
            return Err(Error::InvalidInput("invalid path component"));
        }

        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(22 + src_leaf.len() + dst_leaf.len());
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&src_parent_inode.to_be_bytes());
        payload.push(src_leaf.len() as u8);
        payload.extend_from_slice(src_leaf.as_bytes());
        payload.extend_from_slice(&dst_parent_inode.to_be_bytes());
        payload.push(dst_leaf.len() as u8);
        payload.extend_from_slice(dst_leaf.as_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.push(0);

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_RENAME,
            &payload,
            MATOCL_FUSE_RENAME,
        )?;
        expect_msgid(&packet, msgid)?;
        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: "rename",
                status: packet.payload[4],
            });
        }
        if packet.payload.len() < 8 + 35 {
            return Err(Error::Protocol("short rename reply"));
        }
        Ok(())
    }

    fn open_check(&mut self, inode: u32, flags: u8) -> Result<()> {
        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(21);
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&inode.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.push(flags);

        let packet = roundtrip(&mut self.master, CLTOMA_FUSE_OPEN, &payload, MATOCL_FUSE_OPEN)?;
        expect_msgid(&packet, msgid)?;
        if packet.payload.len() == 5 {
            return Err(Error::MoosefsStatus {
                op: "open",
                status: packet.payload[4],
            });
        }
        if packet.payload.len() < 4 + 35 {
            return Err(Error::Protocol("short open reply"));
        }
        Ok(())
    }

    fn truncate(&mut self, inode: u32, size: u64) -> Result<()> {
        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(29);
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&inode.to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        payload.extend_from_slice(&size.to_be_bytes());

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_TRUNCATE,
            &payload,
            MATOCL_FUSE_TRUNCATE,
        )?;
        if packet.payload.len() == 5 {
            expect_simple_status(&packet, msgid, "truncate")
        } else {
            expect_msgid(&packet, msgid)
        }
    }

    fn master_write_chunk(&mut self, inode: u32, chunk_index: u32) -> Result<ChunkLocation> {
        let master_version = self
            .session
            .as_ref()
            .ok_or(Error::Protocol("session not registered"))?
            .master_version;
        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(13);
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&inode.to_be_bytes());
        payload.extend_from_slice(&chunk_index.to_be_bytes());
        if master_version >= version_int(3, 0, 4) {
            payload.push(CHUNKOPFLAG_CANMODTIME);
        }

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_WRITE_CHUNK,
            &payload,
            MATOCL_FUSE_WRITE_CHUNK,
        )?;
        decode_chunk_location(&packet, msgid, chunk_index)
    }

    fn master_read_chunk(&mut self, inode: u32, chunk_index: u32) -> Result<ChunkLocation> {
        let master_version = self
            .session
            .as_ref()
            .ok_or(Error::Protocol("session not registered"))?
            .master_version;
        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(13);
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&inode.to_be_bytes());
        payload.extend_from_slice(&chunk_index.to_be_bytes());
        if master_version >= version_int(3, 0, 4) {
            payload.push(0);
        }

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_READ_CHUNK,
            &payload,
            MATOCL_FUSE_READ_CHUNK,
        )?;
        decode_chunk_location(&packet, msgid, chunk_index)
    }

    fn master_write_chunk_end(
        &mut self,
        master_version: u32,
        inode: u32,
        chunk_index: u32,
        chunk_id: u64,
        file_size: u64,
        chunk_offset: u32,
        write_size: u32,
    ) -> Result<()> {
        let msgid = self.session_mut()?.next_msgid();
        let mut payload = Vec::with_capacity(37);
        payload.extend_from_slice(&msgid.to_be_bytes());
        payload.extend_from_slice(&chunk_id.to_be_bytes());
        payload.extend_from_slice(&inode.to_be_bytes());
        if master_version >= version_int(3, 0, 74) {
            payload.extend_from_slice(&chunk_index.to_be_bytes());
        }
        payload.extend_from_slice(&file_size.to_be_bytes());
        if master_version >= version_int(3, 0, 4) {
            payload.push(0);
        }
        if master_version >= version_int(4, 40, 0) {
            payload.extend_from_slice(&chunk_offset.to_be_bytes());
            payload.extend_from_slice(&write_size.to_be_bytes());
        }

        let packet = roundtrip(
            &mut self.master,
            CLTOMA_FUSE_WRITE_CHUNK_END,
            &payload,
            MATOCL_FUSE_WRITE_CHUNK_END,
        )?;
        expect_simple_status(&packet, msgid, "write_chunk_end")
    }
}

impl Client<TcpStream> {
    fn chunkserver_read(
        &mut self,
        chunk: &ChunkLocation,
        chunk_offset: u32,
        out: &mut [u8],
    ) -> Result<()> {
        let replica = chunk
            .replicas
            .first()
            .ok_or(Error::Protocol("chunk metadata has no replicas"))?;
        let protocol_id = if replica.chunkserver_version >= version_int(1, 7, 32) {
            Some(1)
        } else {
            None
        };
        let stream = chunkserver_read_session(&mut self.read_session, replica, protocol_id)?;

        let mut request = Vec::with_capacity(21);
        if let Some(protocol_id) = protocol_id {
            request.push(protocol_id);
        }
        request.extend_from_slice(&chunk.chunk_id.to_be_bytes());
        request.extend_from_slice(&chunk.chunk_version.to_be_bytes());
        request.extend_from_slice(&chunk_offset.to_be_bytes());
        request.extend_from_slice(&(out.len() as u32).to_be_bytes());
        write_packet(stream, CLTOCS_READ, &request)?;

        let mut total = 0usize;
        while total < out.len() {
            let packet = read_packet(stream)?;
            if packet.packet_type == CSTOCL_READ_STATUS {
                if packet.payload.len() < 9 {
                    return Err(Error::Protocol("short read status"));
                }
                let status = packet.payload[8];
                if status != 0 {
                    return Err(Error::MoosefsStatus {
                        op: "chunkserver read",
                        status,
                    });
                }
                return Ok(());
            }

            if packet.packet_type != CSTOCL_READ_DATA {
                return Err(Error::Protocol("unexpected chunkserver read reply type"));
            }
            if packet.payload.len() < 20 {
                return Err(Error::Protocol("short read data packet"));
            }

            let block = decode_u16_be(&packet.payload[8..10])? as u32;
            let block_off = decode_u16_be(&packet.payload[10..12])? as u32;
            let data_len = decode_u32_be(&packet.payload[12..16])? as usize;
            let remote_off = block * BLOCK_SIZE + block_off;
            if packet.payload.len() < 20 + data_len {
                return Err(Error::Protocol("truncated read data packet"));
            }

            let copy_off = (chunk_offset as usize).saturating_sub(remote_off as usize);
            if copy_off >= data_len {
                continue;
            }

            let copy_len = (data_len - copy_off).min(out.len() - total);
            out[total..total + copy_len]
                .copy_from_slice(&packet.payload[20 + copy_off..20 + copy_off + copy_len]);
            total += copy_len;
        }

        loop {
            let packet = read_packet(stream)?;
            if packet.packet_type != CSTOCL_READ_STATUS {
                continue;
            }
            if packet.payload.len() < 9 {
                return Err(Error::Protocol("short read status"));
            }
            let status = packet.payload[8];
            if status != 0 {
                return Err(Error::MoosefsStatus {
                    op: "chunkserver read",
                    status,
                });
            }
            return Ok(());
        }
    }

    fn chunkserver_write(
        &mut self,
        chunk: &ChunkLocation,
        chunk_offset: u32,
        bytes: &[u8],
        session: &mut Option<ChunkWriteSession>,
    ) -> Result<()> {
        let replica = chunk
            .replicas
            .first()
            .ok_or(Error::Protocol("chunk metadata has no replicas"))?;
        let protocol_id = if replica.chunkserver_version >= version_int(1, 7, 32) {
            Some(if self.experimental_ooo_write_acks { 0x81 } else { 0x01 })
        } else {
            None
        };
        let stream = chunkserver_write_session(session, replica, protocol_id)?;

        let mut start = Vec::with_capacity(13 + chunk.replicas.len().saturating_sub(1) * 6);
        if let Some(protocol_id) = protocol_id {
            start.push(protocol_id);
        }
        start.extend_from_slice(&chunk.chunk_id.to_be_bytes());
        start.extend_from_slice(&chunk.chunk_version.to_be_bytes());
        for next_replica in chunk.replicas.iter().skip(1) {
            let ip = parse_ipv4_host(&next_replica.host)?;
            start.extend_from_slice(&ip.to_be_bytes());
            start.extend_from_slice(&next_replica.port.to_be_bytes());
        }
        write_packet(stream, CLTOCS_WRITE, &start)?;
        let init_status = recv_write_status(stream, chunk.chunk_id)?;
        if init_status.write_id != 0 {
            return Err(Error::ProtocolDetail(format!(
                "expected initial write status id 0, got {}",
                init_status.write_id
            )));
        }

        let mut sent = 0usize;
        let mut write_id = 1u32;
        let mut in_flight = BTreeMap::new();
        let mut confirmed = ConfirmedRanges::default();
        let required = ByteRange::new(chunk_offset, chunk_offset + bytes.len() as u32);
        let max_in_flight = self.max_in_flight_write_fragments.max(1);
        while sent < bytes.len() || !in_flight.is_empty() {
            while sent < bytes.len() && in_flight.len() < max_in_flight {
                let off = chunk_offset + sent as u32;
                let block = (off / BLOCK_SIZE) as u16;
                let block_offset = (off % BLOCK_SIZE) as u16;
                let frag = (bytes.len() - sent).min((BLOCK_SIZE - block_offset as u32) as usize);
                let fragment = &bytes[sent..sent + frag];

                let mut payload = Vec::with_capacity(24 + frag);
                payload.extend_from_slice(&chunk.chunk_id.to_be_bytes());
                payload.extend_from_slice(&write_id.to_be_bytes());
                payload.extend_from_slice(&block.to_be_bytes());
                payload.extend_from_slice(&block_offset.to_be_bytes());
                payload.extend_from_slice(&(frag as u32).to_be_bytes());
                payload.extend_from_slice(&crc32_update(0, fragment).to_be_bytes());
                payload.extend_from_slice(fragment);

                write_packet(stream, CLTOCS_WRITE_DATA, &payload)?;
                in_flight.insert(write_id, ByteRange::new(off, off + frag as u32));
                sent += frag;
                write_id = write_id.wrapping_add(1);
            }

            if in_flight.is_empty() {
                continue;
            }

            let status = recv_write_status(stream, chunk.chunk_id)?;
            if status.write_id == 0 {
                return Err(Error::Protocol("unexpected extra initial write status"));
            }
            let Some(acked_range) = in_flight.remove(&status.write_id) else {
                return Err(Error::ProtocolDetail(format!(
                    "received write status for unknown write id {}",
                    status.write_id
                )));
            };
            confirmed.insert(acked_range);
        }

        if !confirmed.covers(required) {
            return Err(Error::Protocol("confirmed write coverage is incomplete"));
        }

        let mut finish = Vec::with_capacity(12);
        finish.extend_from_slice(&chunk.chunk_id.to_be_bytes());
        finish.extend_from_slice(&chunk.chunk_version.to_be_bytes());
        write_packet(stream, CLTOCS_WRITE_FINISH, &finish)?;
        Ok(())
    }
}

fn chunkserver_read_session<'a>(
    session: &'a mut Option<ChunkReadSession>,
    replica: &ChunkReplica,
    protocol_id: Option<u8>,
) -> Result<&'a mut TcpStream> {
    let needs_reconnect = match session {
        Some(current) => {
            current.host != replica.host
                || current.port != replica.port
                || current.protocol_id != protocol_id
        }
        None => true,
    };

    if needs_reconnect {
        let stream = TcpStream::connect((replica.host.as_str(), replica.port))?;
        let _ = stream.set_nodelay(true);
        *session = Some(ChunkReadSession {
            host: replica.host.clone(),
            port: replica.port,
            protocol_id,
            stream,
        });
    }

    Ok(&mut session
        .as_mut()
        .ok_or(Error::Protocol("missing chunk read session"))?
        .stream)
}

fn chunkserver_write_session<'a>(
    session: &'a mut Option<ChunkWriteSession>,
    replica: &ChunkReplica,
    protocol_id: Option<u8>,
) -> Result<&'a mut TcpStream> {
    let needs_reconnect = match session {
        Some(current) => {
            current.host != replica.host
                || current.port != replica.port
                || current.protocol_id != protocol_id
        }
        None => true,
    };

    if needs_reconnect {
        let stream = TcpStream::connect((replica.host.as_str(), replica.port))?;
        let _ = stream.set_nodelay(true);
        *session = Some(ChunkWriteSession {
            host: replica.host.clone(),
            port: replica.port,
            protocol_id,
            stream,
        });
    }

    Ok(&mut session
        .as_mut()
        .ok_or(Error::Protocol("missing chunk write session"))?
        .stream)
}

fn validate_absolute_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(Error::InvalidInput("path must not be empty"));
    }
    if !path.starts_with('/') {
        return Err(Error::InvalidInput("path must be absolute"));
    }
    Ok(())
}

fn normalize_master_addr(master_addr: &str) -> String {
    if master_addr.contains(':') {
        master_addr.to_string()
    } else {
        format!("{master_addr}:{DEFAULT_MASTER_PORT}")
    }
}

fn split_parent_and_name(path: &str) -> Result<(&str, &str)> {
    validate_absolute_path(path)?;
    if path == "/" {
        return Err(Error::InvalidInput("path must name a file"));
    }
    let (parent, leaf) = path.rsplit_once('/').unwrap();
    if leaf.is_empty() {
        return Err(Error::InvalidInput("path must not end with '/'"));
    }
    let parent = if parent.is_empty() { "/" } else { parent };
    Ok((parent, leaf))
}

fn parse_ipv4_host(host: &str) -> Result<u32> {
    let mut octets = [0u8; 4];
    let mut count = 0usize;
    for part in host.split('.') {
        if count >= 4 {
            return Err(Error::InvalidInput("replica host is not IPv4"));
        }
        octets[count] = part
            .parse::<u8>()
            .map_err(|_| Error::InvalidInput("replica host is not IPv4"))?;
        count += 1;
    }
    if count != 4 {
        return Err(Error::InvalidInput("replica host is not IPv4"));
    }
    Ok(u32::from_be_bytes(octets))
}

fn decode_chunk_location(
    packet: &crate::protocol::Packet,
    msgid: u32,
    chunk_index: u32,
) -> Result<ChunkLocation> {
    expect_msgid(packet, msgid)?;
    if packet.payload.len() == 5 {
        return Err(Error::MoosefsStatus {
            op: "chunk lookup",
            status: packet.payload[4],
        });
    }
    if packet.payload.len() < 24 {
        return Err(Error::Protocol("short chunk metadata reply"));
    }

    let mut offset = 4usize;
    let protocol_version =
        if packet.payload.len() - offset >= 21 && (1..=3).contains(&packet.payload[offset]) {
            let v = packet.payload[offset];
            offset += 1;
            v
        } else {
            0
        };

    let file_length = decode_u64_be(&packet.payload[offset..offset + 8])?;
    offset += 8;
    let chunk_id = decode_u64_be(&packet.payload[offset..offset + 8])?;
    offset += 8;
    let chunk_version = decode_u32_be(&packet.payload[offset..offset + 4])?;
    offset += 4;

    let entry_size = match protocol_version {
        0 => 6,
        1 => 10,
        _ => 14,
    };

    let mut replicas = Vec::new();
    while packet.payload.len().saturating_sub(offset) >= entry_size {
        let ip = decode_u32_be(&packet.payload[offset..offset + 4])?;
        let port = decode_u16_be(&packet.payload[offset + 4..offset + 6])?;
        let chunkserver_version = if entry_size >= 10 {
            decode_u32_be(&packet.payload[offset + 6..offset + 10])?
        } else {
            0
        };
        let label_mask = if entry_size >= 14 {
            Some(decode_u32_be(&packet.payload[offset + 10..offset + 14])?)
        } else {
            None
        };
        replicas.push(ChunkReplica {
            host: format!(
                "{}.{}.{}.{}",
                (ip >> 24) & 0xff,
                (ip >> 16) & 0xff,
                (ip >> 8) & 0xff,
                ip & 0xff
            ),
            port,
            chunkserver_version,
            label_mask,
        });
        offset += entry_size;
    }

    if replicas.is_empty() {
        return Err(Error::Protocol("chunk metadata has no replicas"));
    }

    Ok(ChunkLocation {
        protocol_version,
        file_length,
        chunk_id,
        chunk_version,
        chunk_index,
        replicas,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WriteStatus {
    write_id: u32,
}

fn recv_write_status(stream: &mut TcpStream, chunk_id: u64) -> Result<WriteStatus> {
    loop {
        let packet = read_packet(stream)?;
        if packet.packet_type != CSTOCL_WRITE_STATUS {
            continue;
        }
        if packet.payload.len() < 13 {
            return Err(Error::Protocol("short write status"));
        }
        if decode_u64_be(&packet.payload[0..8])? != chunk_id {
            return Err(Error::Protocol("write status chunk mismatch"));
        }
        let write_id = decode_u32_be(&packet.payload[8..12])?;
        let status = packet.payload[12];
        if status != 0 {
            return Err(Error::MoosefsStatus {
                op: "chunkserver write",
                status,
            });
        }
        return Ok(WriteStatus { write_id });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_chunk_location, normalize_master_addr, parse_ipv4_host, split_parent_and_name,
        validate_absolute_path, ByteRange, ConfirmedRanges, ConnectOptions, MasterSession,
    };
    use crate::protocol::{Packet, MATOCL_FUSE_WRITE_CHUNK};

    #[test]
    fn default_options_match_docs() {
        let options = ConnectOptions::default();
        assert_eq!(options.client_id, "moosefs-direct");
        assert_eq!(options.subdir, "/");
        assert!(options.password_md5.is_none());
        assert_eq!(options.max_in_flight_write_fragments, 16);
        assert!(!options.experimental_ooo_write_acks);
    }

    #[test]
    fn write_pipeline_options_are_clamped() {
        let options = ConnectOptions::default()
            .with_max_in_flight_write_fragments(0)
            .with_experimental_ooo_write_acks(true);
        assert_eq!(options.max_in_flight_write_fragments, 1);
        assert!(options.experimental_ooo_write_acks);
    }

    #[test]
    fn confirmed_ranges_merge_adjacent_and_overlapping_segments() {
        let mut confirmed = ConfirmedRanges::default();
        confirmed.insert(ByteRange::new(64, 128));
        confirmed.insert(ByteRange::new(0, 64));
        confirmed.insert(ByteRange::new(96, 160));
        assert_eq!(confirmed.ranges, vec![ByteRange::new(0, 160)]);
    }

    #[test]
    fn confirmed_ranges_detect_required_coverage() {
        let mut confirmed = ConfirmedRanges::default();
        confirmed.insert(ByteRange::new(0, 32));
        confirmed.insert(ByteRange::new(32, 96));
        assert!(confirmed.covers(ByteRange::new(0, 96)));
        assert!(!confirmed.covers(ByteRange::new(0, 128)));
    }

    #[test]
    fn session_msgids_increment_from_one() {
        let mut session = MasterSession::new(7, 0x0004_3a03);
        assert_eq!(session.next_msgid(), 1);
        assert_eq!(session.next_msgid(), 2);
    }

    #[test]
    fn master_endpoint_defaults_port() {
        assert_eq!(normalize_master_addr("mfsmaster"), "mfsmaster:9421");
        assert_eq!(normalize_master_addr("mfsmaster:1234"), "mfsmaster:1234");
    }

    #[test]
    fn absolute_paths_are_required() {
        assert!(validate_absolute_path("/ok").is_ok());
        assert!(validate_absolute_path("nope").is_err());
    }

    #[test]
    fn split_parent_keeps_root() {
        assert_eq!(split_parent_and_name("/foo").unwrap(), ("/", "foo"));
        assert_eq!(split_parent_and_name("/a/b").unwrap(), ("/a", "b"));
    }

    #[test]
    fn chunk_location_decode_handles_protocol_1_entries() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&7u32.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&99u64.to_be_bytes());
        payload.extend_from_slice(&1234u64.to_be_bytes());
        payload.extend_from_slice(&55u32.to_be_bytes());
        payload.extend_from_slice(&0x7f00_0001u32.to_be_bytes());
        payload.extend_from_slice(&9422u16.to_be_bytes());
        payload.extend_from_slice(&0x0001_0720u32.to_be_bytes());

        let packet = Packet::new(MATOCL_FUSE_WRITE_CHUNK, payload);
        let decoded = decode_chunk_location(&packet, 7, 0).unwrap();
        assert_eq!(decoded.protocol_version, 1);
        assert_eq!(decoded.file_length, 99);
        assert_eq!(decoded.replicas[0].host, "127.0.0.1");
        assert_eq!(decoded.replicas[0].port, 9422);
    }

    #[test]
    fn ipv4_parser_matches_wire_order() {
        assert_eq!(parse_ipv4_host("10.7.1.195").unwrap(), 0x0a07_01c3);
    }
}
