use std::io::Read;
use std::net::TcpStream;

use crate::{Client, Error, OpenFile, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLayout {
    root: String,
    shard_width: usize,
    shard_levels: usize,
    extension: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub id: String,
    pub path: String,
    pub size: u64,
}

pub struct ObjectStore {
    client: Client<TcpStream>,
    layout: ObjectLayout,
}

impl Default for ObjectLayout {
    fn default() -> Self {
        Self {
            root: "/objects".to_string(),
            shard_width: 2,
            shard_levels: 2,
            extension: ".obj".to_string(),
        }
    }
}

impl ObjectLayout {
    pub fn new(root: &str) -> Result<Self> {
        let mut layout = Self::default();
        layout.root = normalize_root(root)?;
        Ok(layout)
    }

    pub fn with_sharding(mut self, shard_width: usize, shard_levels: usize) -> Self {
        self.shard_width = shard_width;
        self.shard_levels = shard_levels;
        self
    }

    pub fn with_extension(mut self, extension: &str) -> Result<Self> {
        if extension.is_empty() {
            return Err(Error::InvalidInput("object extension must not be empty"));
        }
        if extension.contains('/') {
            return Err(Error::InvalidInput("object extension must not contain '/'"));
        }
        self.extension = extension.to_string();
        Ok(self)
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn object_path(&self, object_id: &str) -> Result<String> {
        validate_object_id(object_id)?;
        let mut path = self.root.clone();
        if self.shard_width > 0 && self.shard_levels > 0 {
            let mut byte_start = 0usize;
            for _ in 0..self.shard_levels {
                if byte_start + self.shard_width > object_id.len() {
                    break;
                }
                path.push('/');
                path.push_str(&object_id[byte_start..byte_start + self.shard_width]);
                byte_start += self.shard_width;
            }
        }
        path.push('/');
        path.push_str(object_id);
        path.push_str(&self.extension);
        Ok(path)
    }

    pub fn object_parent(&self, object_id: &str) -> Result<String> {
        let object_path = self.object_path(object_id)?;
        let (parent, _) = split_parent(&object_path)?;
        Ok(parent.to_string())
    }

    pub fn decode_object_name(&self, name: &str) -> Option<String> {
        name.strip_suffix(&self.extension).map(ToString::to_string)
    }
}

impl ObjectStore {
    pub fn new(client: Client<TcpStream>, layout: ObjectLayout) -> Self {
        Self { client, layout }
    }

    pub fn layout(&self) -> &ObjectLayout {
        &self.layout
    }

    pub fn into_client(self) -> Client<TcpStream> {
        self.client
    }

    pub fn object_path(&self, object_id: &str) -> Result<String> {
        self.layout.object_path(object_id)
    }

    pub fn open_object(&mut self, object_id: &str) -> Result<OpenFile> {
        let path = self.layout.object_path(object_id)?;
        self.client.open_file(&path)
    }

    pub fn ensure_object_len(&mut self, object_id: &str, size: u64) -> Result<OpenFile> {
        let path = self.layout.object_path(object_id)?;
        let parent = self.layout.object_parent(object_id)?;
        self.client.ensure_dir_all(&parent)?;
        self.client.ensure_file_len(&path, size)
    }

    pub fn object_exists(&mut self, object_id: &str) -> Result<bool> {
        let path = self.layout.object_path(object_id)?;
        self.client.path_exists(&path)
    }

    pub fn object_metadata(&mut self, object_id: &str) -> Result<Option<ObjectMetadata>> {
        let path = self.layout.object_path(object_id)?;
        match self.client.stat_path(&path)? {
            Some((_inode, size, file_type)) if file_type == 1 => Ok(Some(ObjectMetadata {
                id: object_id.to_string(),
                path,
                size,
            })),
            Some(_) => Err(Error::Protocol("object path does not resolve to a regular file")),
            None => Ok(None),
        }
    }

    pub fn read_object(&mut self, object_id: &str) -> Result<Vec<u8>> {
        let file = self.open_object(object_id)?;
        let mut out = vec![0u8; file.size as usize];
        self.client.read_at(&file, 0, &mut out)?;
        Ok(out)
    }

    pub fn read_object_at(
        &mut self,
        object_id: &str,
        offset: u64,
        out: &mut [u8],
    ) -> Result<()> {
        let file = self.open_object(object_id)?;
        self.client.read_at(&file, offset, out)
    }

    pub fn write_object(&mut self, object_id: &str, bytes: &[u8]) -> Result<()> {
        let file = self.ensure_object_len(object_id, bytes.len() as u64)?;
        self.client.write_at(&file, 0, bytes)
    }

    pub fn write_object_at(
        &mut self,
        object_id: &str,
        offset: u64,
        bytes: &[u8],
    ) -> Result<()> {
        let required = offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::InvalidInput("object write offset overflow"))?;
        let file = self.ensure_object_len(object_id, required)?;
        self.client.write_at(&file, offset, bytes)
    }

    pub fn write_object_stream<R: Read>(
        &mut self,
        object_id: &str,
        size: u64,
        reader: &mut R,
    ) -> Result<()> {
        let file = self.ensure_object_len(object_id, size)?;
        let mut offset = 0u64;
        let mut buf = vec![0u8; 1024 * 1024];
        while offset < size {
            let remaining = (size - offset) as usize;
            let to_read = remaining.min(buf.len());
            reader.read_exact(&mut buf[..to_read])?;
            self.client.write_at(&file, offset, &buf[..to_read])?;
            offset += to_read as u64;
        }
        Ok(())
    }

    pub fn delete_object(&mut self, object_id: &str) -> Result<()> {
        let path = self.layout.object_path(object_id)?;
        self.client.unlink_path(&path)
    }

    pub fn list_objects_in_shard(&mut self, shard_path: &str) -> Result<Vec<ObjectMetadata>> {
        let full_path = normalize_root(shard_path)?;
        let entries = self.client.list_dir(&full_path)?;
        let mut out = Vec::new();
        for entry in entries {
            if entry.kind().is_dir() {
                continue;
            }
            let Some(id) = self.layout.decode_object_name(&entry.name) else {
                continue;
            };
            out.push(ObjectMetadata {
                id,
                path: format!("{full_path}/{}", entry.name),
                size: entry.size,
            });
        }
        Ok(out)
    }
}

fn normalize_root(root: &str) -> Result<String> {
    if root.is_empty() || !root.starts_with('/') {
        return Err(Error::InvalidInput("object root must be an absolute path"));
    }
    let trimmed = root.trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok("/".to_string());
    }
    Ok(trimmed.to_string())
}

fn split_parent(path: &str) -> Result<(&str, &str)> {
    let slash = path
        .rfind('/')
        .ok_or(Error::InvalidInput("object path must contain a parent directory"))?;
    let parent = if slash == 0 { "/" } else { &path[..slash] };
    let name = &path[slash + 1..];
    if name.is_empty() {
        return Err(Error::InvalidInput("object path must end with a file name"));
    }
    Ok((parent, name))
}

fn validate_object_id(object_id: &str) -> Result<()> {
    if object_id.is_empty() {
        return Err(Error::InvalidInput("object id must not be empty"));
    }
    if object_id == "." || object_id == ".." {
        return Err(Error::InvalidInput("object id must not be '.' or '..'"));
    }
    if object_id.contains('/') {
        return Err(Error::InvalidInput("object id must not contain '/'"));
    }
    if object_id.bytes().any(|b| b < 0x21 || b == b'\\' || b == 0x7f) {
        return Err(Error::InvalidInput(
            "object id must use visible path-safe bytes only",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::ObjectLayout;

    #[test]
    fn object_layout_uses_deterministic_shards() {
        let layout = ObjectLayout::new("/blocks")
            .unwrap()
            .with_sharding(2, 2)
            .with_extension(".blk")
            .unwrap();
        let path = layout
            .object_path("bafybeigdyrzt4examplecidvalue")
            .unwrap();
        assert_eq!(
            path,
            "/blocks/ba/fy/bafybeigdyrzt4examplecidvalue.blk"
        );
    }

    #[test]
    fn object_layout_rejects_nested_ids() {
        let layout = ObjectLayout::new("/blocks").unwrap();
        let err = layout.object_path("cid/with/slash").unwrap_err();
        assert!(err.to_string().contains("must not contain '/'"));
    }

    #[test]
    fn object_layout_decodes_only_matching_extension() {
        let layout = ObjectLayout::new("/blocks")
            .unwrap()
            .with_extension(".blk")
            .unwrap();
        assert_eq!(
            layout.decode_object_name("bafy-object.blk").as_deref(),
            Some("bafy-object")
        );
        assert_eq!(layout.decode_object_name("bafy-object.data"), None);
    }
}
