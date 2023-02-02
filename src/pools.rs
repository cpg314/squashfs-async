//! Readers pools, used when reading data blocks.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fuser_async::{FileHandle, FilesystemSSUS};
use tokio::io::{AsyncSeekExt, BufReader};
#[cfg(feature = "asyncfs")]
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};

use crate::Error;

/// Enumeration of available local reader pools.
#[derive(Clone, Debug, clap::ArgEnum)]
pub enum LocalBackend {
    Tokio,
    #[cfg(feature = "asyncfs")]
    AsyncFs,
    #[cfg(feature = "memmap")]
    MemMap,
}

/// Reader pools for a local backend/filesystem.
pub trait LocalReadersPool: Sized {
    fn new(path: &Path) -> Result<Self, Error>;
}

#[cfg(feature = "asyncfs")]
/// Local readers (backed by [`async_fs::File`])
///
/// This is has difference single- and multi-threaded performance characteristics than
/// [`LocalReadersPool`].
pub struct LocalReadersPoolAsyncFs {
    pub path: PathBuf,
}
#[async_trait::async_trait]
#[cfg(feature = "asyncfs")]
impl deadpool::managed::Manager for LocalReadersPoolAsyncFs {
    type Type = BufReader<Compat<async_fs::File>>;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(BufReader::new(
            async_fs::File::open(&self.path).await?.compat(),
        ))
    }
    async fn recycle(&self, f: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        f.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(())
    }
}
#[cfg(feature = "asyncfs")]
impl LocalReadersPool for LocalReadersPoolAsyncFs {
    fn new(path: &Path) -> Result<Self, Error> {
        Ok(Self { path: path.into() })
    }
}

/// Local readers (backed by [`tokio::fs::File`])
pub struct LocalReadersPoolTokio {
    pub path: PathBuf,
}
#[async_trait::async_trait]
impl deadpool::managed::Manager for LocalReadersPoolTokio {
    type Type = BufReader<tokio::fs::File>;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(BufReader::new(tokio::fs::File::open(&self.path).await?))
    }
    async fn recycle(&self, f: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        f.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(())
    }
}
impl LocalReadersPool for LocalReadersPoolTokio {
    fn new(path: &Path) -> Result<Self, Error> {
        Ok(Self { path: path.into() })
    }
}

#[cfg(feature = "memmap")]
/// Memory-mapped local readers (backed by [`memmap2::Mmap`])
pub struct LocalReadersPoolMemMap {
    pub path: PathBuf,
    data: MemMapArc,
}
/// [`memmap2::Mmap`] wrapped in an [`Arc`]
#[derive(Clone)]
pub struct MemMapArc(Arc<memmap2::Mmap>);
impl AsRef<[u8]> for MemMapArc {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[async_trait::async_trait]
#[cfg(feature = "memmap")]
impl deadpool::managed::Manager for LocalReadersPoolMemMap {
    type Type = std::io::Cursor<MemMapArc>;
    type Error = std::io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        Ok(std::io::Cursor::new(self.data.clone()))
    }
    async fn recycle(&self, _f: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        Ok(())
    }
}
#[cfg(feature = "memmap")]
impl LocalReadersPool for LocalReadersPoolMemMap {
    fn new(path: &Path) -> Result<Self, Error> {
        let file = std::fs::File::open(path).map_err(|_| Error::MemMap)?;
        let data = unsafe { memmap2::Mmap::map(&file).map_err(|_| Error::MemMap)? };
        Ok(Self {
            path: path.into(),
            data: MemMapArc(Arc::new(data)),
        })
    }
}

/// Flags for the `open` syscall
pub type ReadFlags = i32;

/// Readers from [`fuser_async::Filesystem`] file handles.
pub struct FilePool<F: fuser_async::Filesystem>(pub F, pub u64, pub ReadFlags);
#[async_trait::async_trait]
impl<F: FilesystemSSUS + Clone> deadpool::managed::Manager for FilePool<F>
where
    F::Error: Send + Sync + std::fmt::Display + Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Type = BufReader<FileHandle<F>>;
    type Error = tokio::io::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let fh = FileHandle::new(self.0.clone(), self.1, self.2)
            .await
            .map_err(|e| {
                let e: Box<dyn std::error::Error + Send + Sync> = e.into();
                tokio::io::Error::new(tokio::io::ErrorKind::Other, e)
            })?;
        let fh = tokio::io::BufReader::with_capacity(128 * 1024, fh);
        Ok(fh)
    }
    async fn recycle(&self, f: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        f.seek(std::io::SeekFrom::Start(0)).await?;
        Ok(())
    }
}
