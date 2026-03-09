use alloc::string::String;

pub const MFS_SUPER_MAGIC: u32 = 0x4d46_534d;
pub const MFS_BLOCK_SIZE: u32 = 0x0001_0000;
pub const MFS_CHUNK_SIZE: u64 = 0x0400_0000;
pub const MFS_PATH_MAX: usize = 1024;

pub const MFS_DEFAULT_MASTER_PORT: u16 = 9421;
pub const MFS_DEFAULT_MASTER_HOST: &str = "127.0.0.1";
pub const MFS_DEFAULT_SUBDIR: &str = "/";

#[derive(Debug, Clone)]
pub struct MountConfig {
    pub master_host: String,
    pub master_port: u16,
    pub subdir: String,
    pub password: String,
    pub mount_uid: u32,
    pub mount_gid: u32,
}

impl Default for MountConfig {
    fn default() -> Self {
        Self {
            master_host: String::from(MFS_DEFAULT_MASTER_HOST),
            master_port: MFS_DEFAULT_MASTER_PORT,
            subdir: String::from(MFS_DEFAULT_SUBDIR),
            password: String::new(),
            mount_uid: 0,
            mount_gid: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SessionInfo {
    pub session_id: u32,
    pub root_inode: u32,
}
