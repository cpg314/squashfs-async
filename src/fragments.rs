//! Fragments and fragments table.
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::data;
use super::deser;
use super::error::FragmentsError;
use super::metadata;
use super::superblock::SuperBlock;

/// Location in the [`FragmentsTable`]
#[derive(Debug, Default, Copy, Clone, Deserialize)]
pub struct FragmentLocation {
    pub index: u32,
    pub offset: u32,
}
impl FragmentLocation {
    pub fn valid(&self) -> bool {
        self.index != 0xFFFFFFFF
    }
}

/// Fragments table entry
#[derive(Debug, Deserialize)]
pub struct Entry {
    pub start: u64,
    pub size: data::BlockSize,
    _unused: u32,
}
/// Fragments table (a simple list of [`Entry`])
#[derive(Default, Debug)]
pub struct FragmentsTable {
    pub entries: Vec<Entry>,
}
impl std::fmt::Display for FragmentsTable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Fragment table with {} entries", self.entries.len())
    }
}

impl FragmentsTable {
    /// Get an entry from its location
    pub fn entry(&self, location: FragmentLocation) -> Result<&Entry, FragmentsError> {
        if !location.valid() {
            return Err(FragmentsError::InvalidLocation);
        }

        self.entries
            .get(location.index as usize)
            .ok_or(FragmentsError::InvalidLocation)
    }
    /// Read fragments table
    pub async fn from_reader(
        superblock: &SuperBlock,
        mut r: impl crate::AsyncSeekBufRead,
    ) -> Result<Self, FragmentsError> {
        r.seek(std::io::SeekFrom::Start(superblock.fragment_table_start))
            .await
            .map_err(FragmentsError::ReadFailure)?;
        let n = (superblock.fragment_entry_count as f64 / 512.0).ceil() as usize;
        let mut locations = Vec::<u64>::with_capacity(n);
        for _ in 0..n {
            locations.push(
                r.read_u64_le()
                    .await
                    .map_err(|_| FragmentsError::InvalidLocation)?,
            )
        }
        let mut entries = Vec::<Entry>::with_capacity(superblock.fragment_entry_count as usize);
        for l in locations {
            r.seek(std::io::SeekFrom::Start(l))
                .await
                .map_err(FragmentsError::ReadFailure)?;
            let block =
                metadata::MetadataBlock::from_reader(&mut r, superblock.compression).await?;
            entries.extend(
                block
                    .data
                    .chunks(16)
                    .map(deser::bincode_deser)
                    .collect::<Result<Vec<Entry>, _>>()
                    .map_err(|_| FragmentsError::InvalidEntry)?,
            );
        }
        Ok(Self { entries })
    }
}
