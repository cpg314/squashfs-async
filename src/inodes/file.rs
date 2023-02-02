use async_trait::async_trait;
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncReadExt};

use super::super::data::{self, BlockSize};
use super::super::deser;
use super::super::error::InodeTableError;
use super::super::fragments::FragmentLocation;
use super::super::superblock::SuperBlock;

/// Trait to encompass `BasicFile` and `ExtendedFile`.
pub trait FileInode: Send + Sync + std::fmt::Debug {
    fn file_size(&self) -> u64;
    fn blocks_start(&self) -> u64;
    fn add_block_size(&mut self, size: BlockSize);
    fn block_sizes(&self) -> &Vec<BlockSize>;
    fn fragment(&self) -> FragmentLocation;
    fn fragment_size(&self, superblock: &SuperBlock) -> u64 {
        let fragment = self.fragment();
        if !fragment.valid() {
            0
        } else {
            self.file_size() % superblock.block_size as u64
        }
    }
    fn data_locations(&self) -> Box<dyn Iterator<Item = data::DataLocation> + '_> {
        let mut block_start = self.blocks_start();
        Box::new(self.block_sizes().iter().copied().map(move |block_size| {
            let l = data::DataLocation {
                block_start,
                block_size,
            };
            block_start += block_size.compressed_size();
            l
        }))
    }
}
#[async_trait]
pub trait FileInodeDeser: FileInode + serde::de::DeserializeOwned + Sized {
    fn encoded_size() -> usize;
    fn n_blocks(&self, superblock: &SuperBlock) -> u32 {
        let n = self.file_size() as f64 / superblock.block_size as f64;
        if self.fragment().valid() {
            n.floor() as u32
        } else {
            n.ceil() as u32
        }
    }

    async fn from_reader(
        mut r: impl AsyncRead + std::marker::Unpin + Send + Sync,
        superblock: &SuperBlock,
    ) -> Result<Self, InodeTableError> {
        let mut file: Self = deser::bincode_deser_from(&mut r, Self::encoded_size())
            .await
            .map_err(|_| InodeTableError::InvalidEntry)?;
        for _ in 0..file.n_blocks(superblock) {
            file.add_block_size(BlockSize(
                r.read_u32_le()
                    .await
                    .map_err(|_| InodeTableError::InvalidEntry)?,
            ));
        }
        Ok(file)
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct BasicFile {
    blocks_start: u32,
    // serde flatten does not work here
    fragment_index: u32,
    fragment_offset: u32,
    file_size: u32,
    #[serde(skip)]
    block_sizes: Vec<BlockSize>,
}
impl FileInodeDeser for BasicFile {
    fn encoded_size() -> usize {
        16
    }
}

#[async_trait]
impl FileInode for BasicFile {
    fn blocks_start(&self) -> u64 {
        self.blocks_start as u64
    }
    fn add_block_size(&mut self, size: BlockSize) {
        self.block_sizes.push(size)
    }
    fn block_sizes(&self) -> &Vec<BlockSize> {
        &self.block_sizes
    }
    fn file_size(&self) -> u64 {
        self.file_size as u64
    }
    fn fragment(&self) -> FragmentLocation {
        FragmentLocation {
            index: self.fragment_index,
            offset: self.fragment_offset,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ExtendedFile {
    blocks_start: u64,
    file_size: u64,
    _sparse: u64,
    _hard_link_count: u32,
    fragment_index: u32,
    fragment_offset: u32,
    _xattr_idx: u32,
    #[serde(skip)]
    block_sizes: Vec<BlockSize>,
}
impl FileInodeDeser for ExtendedFile {
    fn encoded_size() -> usize {
        40
    }
}
#[async_trait]
impl FileInode for ExtendedFile {
    fn blocks_start(&self) -> u64 {
        self.blocks_start
    }
    fn add_block_size(&mut self, size: BlockSize) {
        self.block_sizes.push(size)
    }
    fn block_sizes(&self) -> &Vec<BlockSize> {
        &self.block_sizes
    }
    fn file_size(&self) -> u64 {
        self.file_size
    }
    fn fragment(&self) -> FragmentLocation {
        FragmentLocation {
            index: self.fragment_index,
            offset: self.fragment_offset,
        }
    }
}
