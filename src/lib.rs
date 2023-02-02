#![doc = include_str!("../README.md")]

mod data;
mod deser;
pub mod directory_table;
pub mod error;
pub mod fragments;
pub mod inodes;
mod metadata;
pub mod pools;
mod squashfuse;
mod superblock;
#[doc(hidden)]
pub mod utils;
use error::CacheError;
pub use error::Error;
use fragments::FragmentsTable;
pub use superblock::{Compression, SuperBlock};

use std::collections::BTreeMap;
use std::fmt::Write;
use std::ops::DerefMut;
use std::path::Path;

use clap::Parser;
use deadpool::managed::Pool;
use fuser_async::cache::{DataBlockCache, IndexCache, LRUCache};
use tokio::sync::RwLock;
use tracing::*;

trait_set::trait_set! {
    /// Convenience trait alias
    pub trait AsyncRead = tokio::io::AsyncRead + Send + Sync + std::marker::Unpin;
    /// Convenience trait alias
    pub trait AsyncSeekBufRead = tokio::io::AsyncSeek + tokio::io::AsyncBufRead + Send + Sync + std::marker::Unpin;

    pub trait ManagerFactory<R> = Fn(pools::ReadFlags) -> Result<R, Error> + Send + Sync + 'static;
}

const TABLES_DIRECT_THRESHOLD: u64 = 50_000;

/// Squashfs reading options.
#[derive(Parser)]
pub struct Options {
    /// Cache size (MB) for decoded blocks.
    #[clap(long, default_value_t = 100)]
    pub cache_mb: u64,
    /// Number of readers
    #[clap(long, default_value_t = 4)]
    pub readers: usize,
    /// Limit (B) for reading small files with direct access.
    ///
    /// This is useful for example when the underlying storage is networked and buffered: for
    /// fast access to small files, one may want to request exactly the data needed, rather than
    /// fetching a whole chunk for buffering.
    ///
    /// This will use another `cache_mb` amount of cache.
    #[clap(long, default_value_t = 0)]
    pub direct_limit: usize,
}

/// Base structure representing a loaded SquashFS image.
///
/// Note that the tables (inode, directory...) are fully parsed on creation and kept in memory,
/// rather than being accessed lazily.
///
/// This implements the [`fuser_async::Filesystem`] trait.
///
/// The type `R` is a [`deadpool`] pool manager for the underlying filesystem readers.
/// See [`crate::pools`].
pub struct SquashFs<R: deadpool::managed::Manager> {
    pub superblock: superblock::SuperBlock,
    pub inode_table: inodes::InodeTable,
    pub fragments_table: FragmentsTable,
    /// Table for each directory inode
    pub directory_tables: BTreeMap<u32 /* inode */, directory_table::DirectoryTable>,
    root_inode: u32,
    pub handles: RwLock<BTreeMap<u64, pools::ReadFlags>>,
    manager_factory: Box<dyn ManagerFactory<R>>,
    readers: RwLock<BTreeMap<pools::ReadFlags, Pool<R>>>,
    n_readers: usize,
    inode_extra: u32,
    /// Files smaller than this size will be accessed with the O_NONBLOCK, which allows triggering
    /// optimizations on the storage backend (e.g. do not pre-fetch a large block for a small file).
    /// See the documentation in [`Options`].
    direct_limit: usize,
    /// Cache for decoded blocks in the image
    cache: Option<IndexCache>,
    /// Cache for small files (< direct_limit), that are read at once.
    small_files_cache: Option<LRUCache>,
}
impl<R: deadpool::managed::Manager> std::fmt::Debug for SquashFs<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "{:?}", self.superblock)?;
        writeln!(f, "{}", self.fragments_table)?;
        writeln!(f, "{}, root inode {}", self.inode_table, self.root_inode)?;
        self.tree(0, self.root_inode, f)?;
        if let Some(cache) = &self.cache {
            writeln!(f, "{}", cache)?;
        }
        if let Some(cache) = &self.small_files_cache {
            writeln!(f, "{}", cache)?;
        }
        Ok(())
    }
}

impl<T, P> SquashFs<P>
where
    T: AsyncSeekBufRead,
    P: pools::LocalReadersPool
        + deadpool::managed::Manager<Type = T, Error = tokio::io::Error>
        + Send
        + Sync,
{
    /// Open squashfs image from a local file
    pub async fn open(file: &Path, options: &Options) -> Result<Self, Error> {
        let file = file.to_owned();
        Self::from_reader(options, move |_| P::new(&file)).await
    }
}

impl<R: deadpool::managed::Manager> SquashFs<R> {
    fn tree<W: Write>(&self, level: usize, root_inode: u32, f: &mut W) -> std::fmt::Result {
        for e in &self.directory_tables.get(&root_inode).unwrap().entries {
            writeln!(f, "{:level$}{}", "", e, level = 4 * level)?;
            if e.is_dir() {
                self.tree(level + 1, e.inode, f)?;
            }
        }
        Ok(())
    }
    pub fn inodes(&self) -> impl Iterator<Item = u32> + '_ {
        self.inode_table
            .files
            .keys()
            .chain(self.inode_table.directories.keys())
            .copied()
    }
}

impl<T, R> SquashFs<R>
where
    T: AsyncSeekBufRead,
    R: deadpool::managed::Manager<Type = T, Error = tokio::io::Error> + Send + Sync,
{
    async fn get_reader(
        &self,
        flags: pools::ReadFlags,
    ) -> Result<deadpool::managed::Object<R>, Error> {
        let readers = self.readers.read().await;
        if !readers.contains_key(&flags) {
            drop(readers);
            let mut readers = self.readers.write().await;
            readers.insert(
                flags,
                Pool::builder((self.manager_factory)(flags)?)
                    .max_size(self.n_readers)
                    .build()?,
            );
            Ok(readers.get(&flags).unwrap().get().await?)
        } else {
            Ok(readers.get(&flags).unwrap().get().await?)
        }
    }
    pub async fn has_handles(&self) -> bool {
        let handles = self.handles.read().await;
        !handles.is_empty()
    }
    /// Open squashfs image from a reader factory, responsible for creating readers with the
    /// requested open flags.
    pub async fn from_reader(
        options: &Options,
        manager_factory: impl ManagerFactory<R>,
    ) -> Result<Self, Error> {
        if options.readers == 0 {
            return Err(Error::InvalidOptions("The number of readers must be >=1"));
        }
        if options.direct_limit as u64 * 10 > options.cache_mb * (1e6 as u64) {
            return Err(Error::InvalidOptions(
                "The cache size must be at least 10x as large as --direct-limit.",
            ));
        }
        let manager_factory = Box::new(manager_factory);

        let mut readers = BTreeMap::<pools::ReadFlags, Pool<R>>::default();
        for flags in [0, libc::O_NONBLOCK] {
            readers.insert(
                flags,
                Pool::builder(manager_factory(flags)?)
                    .max_size(options.readers)
                    .build()?,
            );
        }

        let mut r = readers.get(&libc::O_NONBLOCK).unwrap().get().await?;

        let superblock = superblock::SuperBlock::from_reader(&mut r.deref_mut()).await?;
        debug!(
            "{:?} Tables take {} bytes",
            superblock,
            superblock.tables_length()
        );

        let mut r = if superblock.tables_length() < TABLES_DIRECT_THRESHOLD {
            r
        } else {
            // Don't use direct access if the tables are quite large
            readers.get(&0).unwrap().get().await?
        };
        let mut r = r.deref_mut();
        let root_inode =
            inodes::InodeTable::read_root_inode(superblock.root_inode, &superblock, &mut r).await?;
        let inode_table = inodes::InodeTable::from_reader(&superblock, &mut r).await?;
        let fragments_table = fragments::FragmentsTable::from_reader(&superblock, &mut r).await?;
        let mut directory_table: BTreeMap<u32, directory_table::DirectoryTable> =
            Default::default();

        debug!("Caching directory table");
        for (inode, dir) in &inode_table.directories {
            directory_table.insert(
                *inode,
                directory_table::DirectoryTable::from_reader_directory(
                    dir,
                    &superblock,
                    r.deref_mut(),
                )
                .await?,
            );
        }

        let cache: Option<IndexCache> = if options.cache_mb > 0 {
            let cache: Result<IndexCache, CacheError> = IndexCache::new(
                options.cache_mb,
                superblock.block_size as u64,
                superblock.bytes_used,
            );
            Some(cache?)
        } else {
            None
        };

        let small_files_cache: Option<LRUCache> = if options.direct_limit > 0 {
            let direct_cache: Result<LRUCache, CacheError> = LRUCache::new(
                options.cache_mb,
                options.direct_limit as u64,
                (superblock.inode_count as u64) * (options.direct_limit as u64),
            );
            Some(direct_cache?)
        } else {
            None
        };
        Ok(Self {
            cache,
            small_files_cache,
            inode_extra: inode_table.ids().max().unwrap() + 1,
            superblock,
            n_readers: options.readers,
            directory_tables: directory_table,
            fragments_table,
            inode_table,
            manager_factory,
            root_inode,
            handles: Default::default(),
            readers: RwLock::new(readers),
            direct_limit: options.direct_limit,
        })
    }
}
