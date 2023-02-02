//! Inode table.
//!
//! See <https://dr-emann.github.io/squashfs/squashfs.html#_inode_table>
mod file;
pub use file::FileInode;
use file::FileInodeDeser;
use file::{BasicFile, ExtendedFile};
mod directory;
use directory::{BasicDirectory, ExtendedDirectory};
pub use directory::{DirectoryInode, DirectoryTableLocation};
mod symlink;

use std::collections::BTreeMap;
use std::io::SeekFrom;

use deser::from_reader;
use serde::Deserialize;
use serde_repr::Deserialize_repr;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tracing::*;

use super::deser;
use super::error::InodeTableError;
use super::metadata::MetadataBlock;
use super::superblock::SuperBlock;

/// Reference to an inode, encoding block start and offset.
#[derive(Debug, Copy, Clone, Deserialize)]
pub struct InodeRef(u64);
impl InodeRef {
    fn block_start(&self) -> u64 {
        self.0 >> 16
    }
    fn block_offset(&self) -> u64 {
        self.0 & 0xFFFF
    }
}
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn inoderef_test() {
        let iref = InodeRef(33489312);
        assert_eq!(iref.block_start(), 511);
        assert_eq!(iref.block_offset(), 416);
    }
}

#[derive(Debug, Deserialize_repr)]
#[repr(u16)]
pub enum InodeType {
    BasicDirectory = 1,
    BasicFile,
    BasicSymlink,
    BasicBlockDevice,
    BasicCharDevice,
    BasicFifo,
    BasicSocket,
    ExtendedDirectory,
    ExtendedFile,
    ExtendedSymlink,
    ExtendedBlockDevice,
    ExtendedCharDevice,
    ExtendedFifo,
    ExtendedSocket,
}
impl InodeType {
    pub(crate) fn is_dir(&self) -> bool {
        matches!(self, Self::BasicDirectory | Self::ExtendedDirectory)
    }
}

#[derive(Debug, Deserialize)]
struct InodeHeader {
    inode_type: InodeType,
    _permissions: u16,
    _uid_idx: u16,
    _gid_idx: u16,
    _modified_time: u32,
    inode_number: u32,
}
from_reader!(InodeHeader, 16);

/// Inode table
#[derive(Default, Debug)]
pub struct InodeTable {
    // Change to BTreeMap once
    // https://github.com/rust-lang/rust/pull/102680
    // has been merged.
    // https://github.com/dtolnay/async-trait/issues/215
    pub directories: BTreeMap<u32, Box<dyn DirectoryInode + Send + Sync>>,
    pub files: BTreeMap<u32, Box<dyn FileInode + Send + Sync>>,
}
impl std::fmt::Display for InodeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Inode table with {} directories and {} files",
            self.directories.len(),
            self.files.len()
        )
    }
}
impl InodeTable {
    pub fn ids(&self) -> impl Iterator<Item = u32> + '_ {
        self.directories.keys().chain(self.files.keys()).copied()
    }
    async fn inode_table_bytes<'a>(
        superblock: &'a SuperBlock,
        mut r: impl crate::AsyncSeekBufRead + 'a,
        inode_ref: Option<InodeRef>,
    ) -> Result<impl crate::AsyncRead + 'a, InodeTableError> {
        r.seek(SeekFrom::Start(
            superblock.inode_table_start + inode_ref.map(|x| x.block_start()).unwrap_or_default(),
        ))
        .await
        .map_err(InodeTableError::ReadFailure)?;
        let r = MetadataBlock::from_reader_flatten(
            r,
            superblock.directory_table_start,
            superblock.compression,
        )
        .await?;
        let mut r = Box::pin(r);
        if let Some(inode_ref) = inode_ref {
            let r2 = &mut r;
            tokio::io::copy(
                &mut r2.take(inode_ref.block_offset()),
                &mut tokio::io::sink(),
            )
            .await
            .map_err(InodeTableError::ReadFailure)?;
        }
        Ok(r)
    }
    pub async fn read_root_inode(
        inode_ref: InodeRef,
        superblock: &SuperBlock,
        mut r: impl crate::AsyncSeekBufRead,
    ) -> Result<u32, InodeTableError> {
        let mut r = Self::inode_table_bytes(superblock, &mut r, Some(inode_ref)).await?;
        let header = InodeHeader::from_reader(&mut r)
            .await
            .map_err(|_| InodeTableError::InvalidHeader)?;
        Ok(header.inode_number)
    }
    pub async fn from_reader(
        superblock: &SuperBlock,
        mut r: impl crate::AsyncSeekBufRead,
    ) -> Result<Self, InodeTableError> {
        debug!("Reading inode table");
        let mut table = InodeTable::default();
        let mut r = Self::inode_table_bytes(superblock, &mut r, None).await?;
        loop {
            let mut header = [0; 16];
            let header = match r.read_exact(&mut header).await {
                Ok(_) => InodeHeader::from_reader(&header[..])
                    .await
                    .map_err(|_| InodeTableError::InvalidHeader)?,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(_) => {
                    return Err(InodeTableError::InvalidHeader);
                }
            };
            match header.inode_type {
                InodeType::BasicFile => {
                    let file = BasicFile::from_reader(&mut r, superblock).await?;
                    table.files.insert(header.inode_number, Box::new(file));
                }
                InodeType::ExtendedFile => {
                    let file = ExtendedFile::from_reader(&mut r, superblock).await?;
                    table.files.insert(header.inode_number, Box::new(file));
                }
                InodeType::BasicDirectory => {
                    let dir = BasicDirectory::from_reader(&mut r)
                        .await
                        .map_err(|_| InodeTableError::InvalidEntry)?;
                    table.directories.insert(header.inode_number, Box::new(dir));
                }
                InodeType::ExtendedDirectory => {
                    let dir = ExtendedDirectory::from_reader(&mut r).await?;
                    table.directories.insert(header.inode_number, Box::new(dir));
                }
                InodeType::BasicSymlink => {
                    symlink::Symlink::from_reader(&mut r).await?;
                }
                _ => {
                    warn!("Skipping unsupposed inode of type {:?}", header.inode_type);
                }
            }
        }
        Ok(table)
    }
}
