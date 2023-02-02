//! Reading data blocks
use std::ops::DerefMut;

use async_compression::tokio::bufread::{XzDecoder, ZlibDecoder, ZstdDecoder};
use fuser_async::cache::DataBlockCache;
use fuser_async::utils::OutOf;
use serde::Deserialize;
use tokio::io::{AsyncBufRead, AsyncReadExt, AsyncSeekExt, AsyncWrite};
use tracing::*;

use super::error::DecompressError;
use super::superblock::Compression;
use super::Error;
use super::SquashFs;
use crate::pools;

pub async fn decompress(
    mut input: impl AsyncBufRead + Unpin + Send + Sync,
    compressed_size: u64,
    mut output: impl AsyncWrite + Unpin,
    compression: Option<Compression>,
) -> Result<(), DecompressError> {
    let mut input = (&mut input).take(compressed_size);

    let mut input: Box<dyn crate::AsyncRead> = match compression {
        None => Box::new(input),
        Some(Compression::Zstd) => Box::new(ZstdDecoder::new(&mut input)),
        Some(Compression::Gzip) => Box::new(ZlibDecoder::new(&mut input)),
        Some(Compression::Xz) => Box::new(XzDecoder::new(&mut input)),
        // TODO: Other schemes
        Some(compression) => return Err(DecompressError::UnsupportedCompression(compression)),
    };
    tokio::io::copy(&mut input, &mut output).await?;

    Ok(())
}

#[derive(Debug, Copy, Clone, Deserialize)]
pub struct BlockSize(pub u32);
impl BlockSize {
    pub fn compressed(&self) -> bool {
        (self.0 & (1 << 24)) == 0
    }
    pub fn compressed_size(&self) -> u64 {
        (self.0 & 0x00FFFFFF) as u64
    }
}

#[derive(Debug)]
pub struct DataLocation {
    pub block_start: u64,
    pub block_size: BlockSize,
}

impl<
        T: crate::AsyncSeekBufRead,
        R: deadpool::managed::Manager<Type = T, Error = tokio::io::Error> + Send + Sync,
    > SquashFs<R>
{
    /// Read from a file from the archive
    pub async fn read_file(
        &self,
        inode: u32,
        offset: usize,
        size: usize,
        mut flags: pools::ReadFlags,
        compression: Compression,
    ) -> Result<bytes::Bytes, Error> {
        let file = self
            .inode_table
            .files
            .get(&inode)
            .ok_or(Error::FileNotFound(None))?;
        let size = size.min(
            (file.file_size() as usize)
                .checked_sub(offset)
                .ok_or(Error::InvalidOffset)?,
        );
        if size == 0 {
            return Ok(bytes::Bytes::default());
        }
        debug!(
            inode,
            offset,
            size,
            portion = OutOf::new(size, file.file_size()).display_full(),
            "Reading squashfs file",
        );
        // Optimization for small files
        if (file.file_size() as usize) < self.superblock.block_size as usize {
            // Treating these separately also allows not having to worry about fragments below.
            warn!(inode, "Accessing very small file (< block) in direct mode");
            flags |= libc::O_DIRECT;
        } else if let (true, Some(cache)) = (
            (file.file_size() as usize) < self.direct_limit
                // Skip when tailend fragments (which would require another fetch)
                && !file.fragment().valid() && (flags & libc::O_DIRECT) != 0,
            &self.small_files_cache,
        ) {
            // We read the entire underlying data at once and then decode it.
            // This large read, along with the O_DIRECT flag, provides a hint to the backend
            // to skip buffering and read exactly this data.
            let first = file.data_locations().next().unwrap();
            let tot_size = file
                .data_locations()
                .map(|dl| dl.block_size.compressed_size())
                .sum::<u64>();
            flags |= libc::O_DIRECT;
            // Cache the entire decompressed file
            let cached = cache
                .insert_lock(inode as u64, async {
                    warn!(
                        inode,
                        "Accessing small file (< direct limit) in direct mode"
                    );
                    let mut reader = self.get_reader(flags).await?;
                    // Read the raw contents
                    reader
                        .seek(std::io::SeekFrom::Start(first.block_start))
                        .await
                        .map_err(Error::ReadFailure)?;
                    let mut buf = bytes::BytesMut::zeroed(tot_size as usize);
                    reader
                        .read_exact(&mut buf)
                        .await
                        .map_err(Error::ReadFailure)?;
                    let mut cursor = std::io::Cursor::new(buf.deref_mut());
                    // Decode the contents
                    self.read_file_impl(
                        file,
                        (&mut cursor, first.block_start),
                        inode,
                        // Use the decompressed size here
                        (0, file.file_size() as usize),
                        compression,
                    )
                    .await
                    .map_err(Box::new)
                })
                .await?;
            let mut buf = bytes::BytesMut::zeroed(size);
            buf.copy_from_slice(&cached.data[offset..offset + size]);
            return Ok(buf.into());
        }
        let mut reader = self.get_reader(flags).await?;
        self.read_file_impl(
            file,
            (reader.deref_mut(), 0),
            inode,
            (offset, size),
            compression,
        )
        .await
    }
    #[allow(clippy::borrowed_box)]
    pub async fn read_file_impl(
        &self,
        file: &Box<dyn crate::inodes::FileInode + Send + Sync>,
        (mut reader, reader_offset): (impl crate::AsyncSeekBufRead, u64),
        inode: u32,
        (offset, size): (usize, usize),
        compression: Compression,
    ) -> Result<bytes::Bytes, Error> {
        let start = std::time::Instant::now();

        let superblock = &self.superblock;
        let first_block = (offset as f64 / superblock.block_size as f64).floor() as usize;
        let block_offset = offset % self.superblock.block_size as usize;
        let n_blocks =
            ((block_offset + size) as f64 / self.superblock.block_size as f64).ceil() as usize;
        let mut buf = bytes::BytesMut::zeroed(superblock.block_size as usize * n_blocks);
        let mut buf_parts: Vec<_> = (0..n_blocks)
            .map(|_| buf.split_off(buf.len() - superblock.block_size as usize))
            .rev()
            .collect();

        let data_locations: Vec<_> = file
            .data_locations()
            .skip(first_block)
            .take(n_blocks)
            .collect();
        debug!(
            inode,
            offset,
            size,
            "{} data blocks to read, {} available from regular blocks, first block {}",
            n_blocks,
            data_locations.len(),
            first_block
        );
        // Read from regular data blocks
        for (l, buf_part) in data_locations.iter().zip(buf_parts.iter_mut()) {
            read_data_block(
                &mut reader,
                reader_offset,
                l.block_start,
                l.block_size,
                buf_part.as_mut(),
                self.cache.as_ref(),
                compression,
            )
            .await?;
        }
        // Read last part from fragment if necessary
        if data_locations.len() != n_blocks {
            debug!("Reading from fragment");
            assert!(n_blocks == data_locations.len() + 1);
            let buf = buf_parts.last_mut().unwrap();
            let fragment_location = file.fragment();
            let entry = self.fragments_table.entry(fragment_location)?;

            read_data_block(
                reader,
                reader_offset,
                entry.start,
                entry.size,
                buf,
                self.cache.as_ref(),
                compression,
            )
            .await?;
            let _ = buf.split_to(fragment_location.offset as usize);
        }
        for part in buf_parts {
            buf.unsplit(part);
        }
        let _ = buf.split_to(block_offset);
        let _ = buf.split_off(size);
        let buf = buf.freeze();

        if buf.len() != size {
            return Err(Error::InvalidBufferSize);
        }
        debug!(
            inode,
            offset,
            size,
            speed_mb_s = buf.len() as f64 / 1e6 / start.elapsed().as_secs_f64(),
            "Finished reading",
        );
        Ok(buf)
    }
}
pub async fn read_data_block(
    mut r: impl crate::AsyncSeekBufRead,
    reader_offset: u64,
    start: u64,
    b: BlockSize,
    buf: &mut [u8],
    cache: Option<&impl DataBlockCache<Box<Error>>>,
    compression: Compression,
) -> Result<(), Error> {
    r.seek(std::io::SeekFrom::Start(start - reader_offset))
        .await
        .map_err(Error::ReadFailure)?;
    debug!(
        compression_ratio = b.compressed_size() as f32 / buf.len() as f32,
        compressed_size = b.compressed_size(),
        start,
        cache = format!("{}", cache.map(|c| c.to_string()).unwrap_or_default()),
        "Reading data block",
    );

    // Crucial to not mess up the caching
    if b.compressed_size() == 0 {
        return Ok(());
    }
    // Check cache
    if let Some(cache) = cache {
        if let Some(block) = cache.get(start).await {
            if block.data.len() != buf.len() {
                return Err(Error::InvalidBufferSize);
            }
            buf.copy_from_slice(&block.data);
            return Ok(());
        }
    }
    // Given we're reading directly into the buffer, we're not doing that in the lock insert.
    // (but we might be missing some cache hits doing so)
    let mut cursor = std::io::Cursor::new(buf);

    decompress(
        &mut r,
        b.compressed_size(),
        &mut cursor,
        b.compressed().then_some(compression),
    )
    .await?;
    // Write cache
    if let Some(cache) = cache {
        let _ = cache
            .insert_lock(start, async {
                let data = cursor.into_inner();
                Ok(&*data)
            })
            .await?;
    }
    Ok(())
}
