use serde::Deserialize;

use super::super::deser::{self, from_reader};
use super::super::error::InodeTableError;

/// Location in the directory table.
#[derive(Default, Debug, Clone)]
pub struct DirectoryTableLocation {
    pub start: u64,
    pub offset: u64,
    pub file_size: u64,
}
/// Trait to encompass `BasicDirectory` and `ExtendedDirectory`
pub trait DirectoryInode: std::fmt::Debug {
    fn hard_link_count(&self) -> u32;
    fn parent_inode_number(&self) -> u32;
    fn table_location(&self) -> DirectoryTableLocation;
}
#[derive(Debug, Default, Deserialize)]
pub struct BasicDirectory {
    dir_block_start: u32,
    hard_link_count: u32,
    file_size: u16,
    block_offset: u16,
    parent_inode_number: u32,
}
from_reader!(BasicDirectory, 16);
impl DirectoryInode for BasicDirectory {
    fn hard_link_count(&self) -> u32 {
        self.hard_link_count
    }
    fn parent_inode_number(&self) -> u32 {
        self.parent_inode_number
    }
    fn table_location(&self) -> DirectoryTableLocation {
        DirectoryTableLocation {
            start: self.dir_block_start as u64,
            offset: self.block_offset as u64,
            file_size: self.file_size as u64,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct DirectoryIndex {
    _index: u32,
    _start: u32,
    name_size: u32,
    #[serde(skip)]
    name: String,
}
impl DirectoryIndex {
    pub async fn from_reader(mut r: impl crate::AsyncRead) -> Result<Self, InodeTableError> {
        let mut index: Self = deser::bincode_deser_from(&mut r, 12)
            .await
            .map_err(|_| InodeTableError::InvalidEntry)?;
        index.name = deser::bincode_deser_string_from(r, index.name_size as usize + 1)
            .await
            .map_err(|_| InodeTableError::InvalidEntry)?;
        Ok(index)
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ExtendedDirectory {
    hard_link_count: u32,
    file_size: u32,
    dir_block_start: u32,
    parent_inode_number: u32,
    index_count: u16,
    block_offset: u16,
    _xattr_idx: u32,
    #[serde(skip)]
    index: Vec<DirectoryIndex>,
}
impl DirectoryInode for ExtendedDirectory {
    fn hard_link_count(&self) -> u32 {
        self.hard_link_count
    }
    fn parent_inode_number(&self) -> u32 {
        self.parent_inode_number
    }
    fn table_location(&self) -> DirectoryTableLocation {
        DirectoryTableLocation {
            start: self.dir_block_start as u64,
            offset: self.block_offset as u64,
            file_size: self.file_size as u64,
        }
    }
}
impl ExtendedDirectory {
    pub async fn from_reader(mut r: impl crate::AsyncRead) -> Result<Self, InodeTableError> {
        let mut dir: Self = deser::bincode_deser_from(&mut r, 24)
            .await
            .map_err(|_| InodeTableError::InvalidEntry)?;
        for _ in 0..dir.index_count {
            dir.index.push(DirectoryIndex::from_reader(&mut r).await?);
        }
        Ok(dir)
    }
}
