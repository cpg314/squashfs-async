use serde::Deserialize;
use serde_repr::Deserialize_repr;
use tracing::*;

use super::error::DecompressError;
use super::inodes::InodeRef;
use super::metadata::MetadataBlock;
use super::Error;

/// Compression algorithm
#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize_repr)]
#[repr(u16)]
pub enum Compression {
    Gzip = 1,
    Lzma,
    Lzd,
    Xz,
    Lz4,
    Zstd,
}

bitflags::bitflags! {
    #[derive(Deserialize)]
    struct SuperBlockFlags: u16 {
        const UNCOMPRESSED_INODES = 0x0001;
        const UNCOMPRESSED_DATA = 0x0002;
        const CHECK = 0x0004;
        const UNCOMPRESSED_FRAGMENTS = 0x0008;
        const NO_FRAGMENTS = 0x0010;
        const ALWAYS_FRAGMENTS = 0x0020;
        const DUPLICATES = 0x0040;
        const EXPORTABLE = 0x0080;
        const UNCOMPRESSED_XATTRS = 0x0100;
        const NO_XATTRS = 0x0200;
        const COMPRESSOR_OPTIONS = 0x0400;
        const UNCOMPRESSED_IDS = 0x0800;
    }
}
#[derive(Debug)]
pub enum CompressionOptions {
    Zstd,
    Gzip,
    Xz,
}
impl CompressionOptions {
    fn from_metadata(compression: Compression, block: MetadataBlock) -> Result<Self, Error> {
        match compression {
            Compression::Zstd => {
                if block.compressed_size != 4 {
                    return Err(Error::InvalidBufferSize);
                }
                Ok(Self::Zstd)
            }
            Compression::Gzip => {
                if block.compressed_size != 8 {
                    return Err(Error::InvalidBufferSize);
                }
                Ok(Self::Gzip)
            }
            Compression::Xz => {
                if block.compressed_size != 8 {
                    return Err(Error::InvalidBufferSize);
                }
                Ok(Self::Xz)
            }
            // TODO: Other compression schemes
            _ => Err(DecompressError::UnsupportedCompression(compression).into()),
        }
    }
}
/// Superblock, containing archive metadata.
///
/// See <https://dr-emann.github.io/squashfs/squashfs.html#_the_superblock>
#[derive(Debug, Deserialize)]
pub struct SuperBlock {
    magic: u32,
    pub inode_count: u32,
    _modification_time: u32,
    pub block_size: u32,
    pub fragment_entry_count: u32,
    pub compression: Compression,
    _block_log: u16,
    flags: SuperBlockFlags,
    _id_lookupcount: u16,
    version_major: u16,
    version_minor: u16,
    pub root_inode: InodeRef,
    /// Without padding
    pub bytes_used: u64,
    _id_table_start: u64,
    _xattr_id_table_start: u64,
    pub inode_table_start: u64,
    pub directory_table_start: u64,
    pub fragment_table_start: u64,
    _export_table_start: u64,
    #[serde(skip)]
    pub compression_options: Option<CompressionOptions>,
}
impl SuperBlock {
    pub async fn from_reader(mut r: impl crate::AsyncSeekBufRead) -> Result<Self, Error> {
        debug!("Reading superblock");
        let mut superblock: Self = super::deser::bincode_deser_from(&mut r, 96)
            .await
            .map_err(|_| Error::InvalidSuperblock)?;

        if superblock.magic != 0x73717368
            || superblock.version_major != 4
            || superblock.version_minor != 0
        {
            return Err(Error::InvalidSuperblock);
        }
        if superblock
            .flags
            .contains(SuperBlockFlags::COMPRESSOR_OPTIONS)
        {
            let block = MetadataBlock::from_reader(&mut r, superblock.compression).await?;
            superblock.compression_options = Some(CompressionOptions::from_metadata(
                superblock.compression,
                block,
            )?);
        }
        debug!("{:?}", superblock);
        Ok(superblock)
    }
    pub fn tables_length(&self) -> u64 {
        self.bytes_used - self.inode_table_start
    }
}
