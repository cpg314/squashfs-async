//! Implementation of `fuse_async::Filesystem` on `SquashFs`.
use std::collections::BTreeSet;
use std::time::UNIX_EPOCH;

use fuser_async::Error as ErrorFuse;
use fuser_async::{utils::BLOCK_SIZE, DirEntry};

use crate::{Error, SquashFs};

impl From<&super::directory_table::Entry> for DirEntry {
    fn from(e: &super::directory_table::Entry) -> Self {
        DirEntry {
            inode: e.inode as u64,
            name: e.name.clone(),
            file_type: if e.is_dir() {
                fuser::FileType::Directory
            } else {
                fuser::FileType::RegularFile
            },
        }
    }
}

impl<R: deadpool::managed::Manager> SquashFs<R> {
    /// Remapping to ensure that the root inode is `fuser::FUSE_ROOT_ID`
    fn ino_from_fuse(&self, ino: u64) -> Result<u32, Error> {
        if ino == fuser::FUSE_ROOT_ID {
            Ok(self.root_inode)
        } else if ino == self.inode_extra as u64 {
            let fuse_root: u32 = fuser::FUSE_ROOT_ID.try_into().unwrap();
            Ok(fuse_root)
        } else {
            ino.try_into().map_err(|_| Error::InvalidInode)
        }
    }
    /// Remapping to ensure that the root inode is `fuser::FUSE_ROOT_ID`
    pub fn ino_to_fuse(&self, ino: u32) -> u64 {
        let fuse_root: u32 = fuser::FUSE_ROOT_ID.try_into().unwrap();
        if ino == self.root_inode {
            fuser::FUSE_ROOT_ID
        } else if ino == fuse_root {
            self.inode_extra as u64
        } else {
            ino as u64
        }
    }
    fn getattr_inode(&self, ino: u32) -> Result<fuser::FileAttr, Error> {
        if let Some(f) = self.inode_table.files.get(&ino) {
            Ok(fuser_async::utils::file_attr(
                self.ino_to_fuse(ino),
                f.file_size(),
                UNIX_EPOCH,
            ))
        } else {
            let directory = self
                .inode_table
                .directories
                .get(&ino)
                .ok_or(Error::DirectoryNotFound)?;
            Ok(fuser::FileAttr {
                ino: self.ino_to_fuse(ino),
                size: 0,
                blocks: 0,
                // TODO: Set these.
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: fuser::FileType::Directory,
                perm: 0o755,
                nlink: directory.hard_link_count(),
                uid: 501,
                gid: 20,
                rdev: 0,
                flags: 0,
                blksize: BLOCK_SIZE,
            })
        }
    }
}

#[async_trait::async_trait]
impl<
        T: crate::AsyncSeekBufRead,
        R: deadpool::managed::Manager<Type = T, Error = tokio::io::Error> + Send + Sync,
    > fuser_async::Filesystem for SquashFs<R>
{
    type Error = Error;
    async fn inodes(&self) -> Result<BTreeSet<u64>, Error> {
        Ok(self.inodes().map(|ino| self.ino_to_fuse(ino)).collect())
    }

    async fn open(&self, _ino: u64, flags: i32) -> Result<u64, Self::Error> {
        let mut handles = self.handles.write().await;
        let fh = handles.keys().last().copied().unwrap_or_default() + 1;
        handles.insert(fh, flags);
        Ok(fh)
    }
    async fn release(&self, _ino: u64, fh: u64) -> Result<(), Self::Error> {
        let mut handles = self.handles.write().await;
        handles
            .remove(&fh)
            .ok_or(Error::Fuse(fuser_async::Error::BadFileDescriptor))?;
        Ok(())
    }

    async fn lookup(&self, parent: u64, name: &std::ffi::OsStr) -> Result<fuser::FileAttr, Error> {
        let ino = self.ino_from_fuse(parent)?;
        let d = self
            .directory_tables
            .get(&ino)
            .ok_or(Error::DirectoryNotFound)?;
        let name = name.to_str().ok_or(Error::Encoding)?;
        let f = d
            .find(name)
            .ok_or_else(|| Error::FileNotFound(Some(name.into())))?;
        Ok(self.getattr_inode(f.inode)?)
    }
    async fn getattr(&self, ino_fuse: u64) -> Result<fuser::FileAttr, Error> {
        let ino = self.ino_from_fuse(ino_fuse)?;
        self.getattr_inode(ino)
    }
    async fn setattr(
        &mut self,
        _ino: u64,
        _size: Option<u64>,
    ) -> Result<fuser::FileAttr, Self::Error> {
        Err(ErrorFuse::Unimplemented.into())
    }
    async fn readdir(
        &self,
        ino_fuse: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = fuser_async::DirEntry> + Send + Sync + '_>, Error> {
        let ino = self.ino_from_fuse(ino_fuse).unwrap();
        let d = self
            .directory_tables
            .get(&ino)
            .ok_or(Error::DirectoryNotFound)?;
        Ok(Box::new(
            d.entries
                .iter()
                .skip(offset as usize)
                .map(fuser_async::DirEntry::from)
                .map(|mut e| {
                    e.inode = self.ino_to_fuse(e.inode as u32);
                    e
                }),
        ))
    }
    async fn read(
        &self,
        ino_fuse: u64,
        fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Error> {
        let ino = self.ino_from_fuse(ino_fuse)?;
        let flags = {
            let handles = self.handles.read().await;
            *handles
                .get(&fh)
                .ok_or(Error::Fuse(fuser_async::Error::BadFileDescriptor))?
        };
        Ok(self
            .read_file(
                ino,
                offset as usize,
                size as usize,
                flags,
                self.superblock.compression,
            )
            .await?)
    }
    async fn write(
        &self,
        _ino: u64,
        _fh: u64,
        _data: bytes::Bytes,
        _offset: i64,
    ) -> Result<u32, Self::Error> {
        Err(ErrorFuse::ReadOnly.into())
    }
    async fn create(
        &mut self,
        _parent: u64,
        _name: std::ffi::OsString,
        _mode: u32,
        _umask: u32,
        _flags: i32,
    ) -> Result<(fuser::FileAttr, u64), Self::Error> {
        Err(ErrorFuse::ReadOnly.into())
    }
    async fn mkdir(
        &mut self,
        _parent: u64,
        _name: std::ffi::OsString,
    ) -> Result<fuser::FileAttr, Self::Error> {
        Err(ErrorFuse::ReadOnly.into())
    }
}
