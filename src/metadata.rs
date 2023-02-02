use futures::stream::{self, Stream, StreamExt, TryStreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing::*;

use super::data::decompress;
use super::error::MetadataError;
use super::superblock::Compression;

/// https://dr-emann.github.io/squashfs/squashfs.html#_packing_metadata
#[derive(Debug)]
pub struct MetadataBlock {
    pub compressed_size: u16,
    pub data: Vec<u8>,
}
impl MetadataBlock {
    pub async fn from_reader(
        mut r: impl crate::AsyncSeekBufRead,
        compression: Compression,
    ) -> Result<Self, MetadataError> {
        let header = r
            .read_u16_le()
            .await
            .map_err(|_| MetadataError::InvalidHeader)?;
        let compressed_size = header & 0x7FFF;
        let compressed = (header & 0x8000) == 0;
        debug!("Read metadata block of size {}", compressed_size);
        let mut data = Vec::<u8>::with_capacity(8192);
        let mut cursor = std::io::Cursor::new(&mut data);
        decompress(
            &mut r,
            compressed_size as u64,
            &mut cursor,
            compressed.then_some(compression),
        )
        .await?;
        if data.len() > 8192 {
            return Err(MetadataError::InvalidDataLength);
        }
        Ok(Self {
            data,
            compressed_size,
        })
    }
    pub fn from_reader_stream<'a>(
        mut r: impl crate::AsyncSeekBufRead + 'a,
        end: u64,
        compression: Compression,
    ) -> impl Stream<Item = Result<(u64, Self), MetadataError>> + 'a {
        async_stream::stream! {
            loop {
                let pos = r.stream_position().await.map_err(MetadataError::ReadFailure)?;
                if pos >= end {
                    break;
                }
                let block = MetadataBlock::from_reader(&mut r, compression).await?;
                yield Ok((pos, block));
            }
        }
    }
    pub async fn from_reader_flatten<'a>(
        r: impl crate::AsyncSeekBufRead + 'a,
        end: u64,
        compression: Compression,
    ) -> Result<impl AsyncRead + 'a, MetadataError> {
        Ok(Self::from_reader_stream(r, end, compression)
            .map_ok(|(_, b)| stream::iter(b.data).map(|x| Ok([x])))
            .try_flatten()
            .map_err(|e: MetadataError| std::io::Error::new(std::io::ErrorKind::Other, e))
            .into_async_read()
            .compat())
    }
}
