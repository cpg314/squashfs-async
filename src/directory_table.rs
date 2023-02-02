//! Directory table parsing
//!
//! See <https://dr-emann.github.io/squashfs/squashfs.html#_directory_table>
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::SeekFrom;

use deser::from_reader;
use itertools::Itertools;
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::*;

use super::deser;
use super::error::DirectoryTableError;
use super::inodes::{DirectoryInode, InodeType};
use super::metadata::MetadataBlock;
use super::superblock::SuperBlock;

#[derive(Debug, Deserialize)]
struct Header {
    entries: u32,
    inode_table_offset: u32,
    inode_number_base: u32,
}
from_reader!(Header, 12);

#[derive(Debug, Deserialize)]
struct EntryInternal {
    inode_metadata_offset: u16,
    inode_offset: i16,
    r#type: InodeType,
    name_size: u16,
    #[serde(skip)]
    name: String,
}

/// Directory table entry
#[derive(Debug)]
pub struct Entry {
    _inode_metadata_offset: u32,
    pub inode: u32,
    pub r#type: InodeType,
    pub name: String,
}
impl Entry {
    pub fn is_dir(&self) -> bool {
        self.r#type.is_dir()
    }
}
impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.name,
            self.is_dir().then_some("/").unwrap_or_default()
        )
    }
}
impl Entry {
    fn from(header: &Header, entry: EntryInternal) -> Self {
        Self {
            _inode_metadata_offset: header.inode_table_offset + entry.inode_metadata_offset as u32,
            name: entry.name,
            r#type: entry.r#type,
            inode: (header.inode_number_base as i32 + entry.inode_offset as i32) as u32,
        }
    }
}
impl EntryInternal {
    async fn from_reader(mut r: impl crate::AsyncRead) -> Result<Self, DirectoryTableError> {
        let mut entry: Self = deser::bincode_deser_from(&mut r, 8)
            .await
            .map_err(|_| DirectoryTableError::InvalidEntry)?;
        entry.name = deser::bincode_deser_string_from(r, entry.name_size as usize + 1)
            .await
            .map_err(|_| DirectoryTableError::InvalidEntry)?;
        Ok(entry)
    }
}

fn index_hash(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}
/// Table for one directory
#[derive(Default, Debug)]
pub struct DirectoryTable {
    pub entries: Vec<Entry>,
    /// inode-to-index (into `entries` for fast access)
    index: HashMap<u64, Vec<usize>>,
}
impl DirectoryTable {
    pub fn find(&self, name: &str) -> Option<&Entry> {
        self.index
            .get(&index_hash(name))
            .into_iter()
            .flatten()
            .map(|i| &self.entries[*i])
            .find(|e| e.name == name)
    }
    async fn from_reader(mut r: impl crate::AsyncRead) -> Result<Self, DirectoryTableError> {
        // Read entries
        let mut entries = vec![];
        let mut header = [0; 12];
        loop {
            // Read header
            let header = match r.read_exact(&mut header).await {
                Ok(_) => Header::from_reader(&header[..])
                    .await
                    .map_err(|_| DirectoryTableError::InvalidHeader)?,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(_) => return Err(DirectoryTableError::InvalidHeader),
            };
            debug!("Directory table header {:?}", header);
            // Read entries
            for _ in 0..header.entries + 1 {
                let entry = EntryInternal::from_reader(&mut r).await?;
                entries.push(Entry::from(&header, entry));
            }
        }
        Ok(DirectoryTable {
            index: entries
                .iter()
                .enumerate()
                .map(|(i, e)| (index_hash(&e.name), i))
                .into_group_map(),
            entries,
        })
    }
    #[allow(clippy::borrowed_box)]
    pub async fn from_reader_directory(
        directory: &Box<dyn DirectoryInode + Send + Sync>,
        superblock: &SuperBlock,
        mut r: impl crate::AsyncSeekBufRead,
    ) -> Result<Self, DirectoryTableError> {
        let loc = directory.table_location();
        r.seek(SeekFrom::Start(
            superblock.directory_table_start + loc.start,
        ))
        .await
        .map_err(DirectoryTableError::ReadFailure)?;
        let r = MetadataBlock::from_reader_flatten(
            r,
            superblock.fragment_table_start,
            superblock.compression,
        )
        .await?;
        let mut r = Box::pin(r);
        // Get the section of the metadata block corresponding to the directory
        let r2 = &mut r;
        tokio::io::copy(&mut r2.take(loc.offset), &mut tokio::io::sink())
            .await
            .map_err(DirectoryTableError::ReadFailure)?;
        let r = r.take(loc.file_size);
        Self::from_reader(r).await
    }
}
