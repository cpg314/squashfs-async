//! SquashFS reading error.
use fuser_async::Error as ErrorFuse;

use crate::superblock::Compression;

pub(crate) type CacheError = fuser_async::cache::CacheError<Box<Error>>;

/// Main error type.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid buffer size")]
    InvalidBufferSize,
    #[error("Invalid superblock")]
    InvalidSuperblock,
    #[error("Read failure")]
    ReadFailure(std::io::Error),
    #[error("File not found: {0:?}")]
    FileNotFound(Option<String>),
    #[error("Directory not found")]
    DirectoryNotFound,
    #[error("Invalid file offset")]
    InvalidOffset,
    #[error("Cache error: {source}")]
    CacheError {
        #[from]
        source: CacheError,
    },
    #[error("Readers pool error: {source}")]
    PoolError {
        #[from]
        source: deadpool::managed::PoolError<std::io::Error>,
    },
    #[error("Readers pool creation error: {source}")]
    PoolBuildError {
        #[from]
        source: deadpool::managed::BuildError<std::io::Error>,
    },
    #[error("Invalid options: {0}")]
    InvalidOptions(&'static str),
    #[error("Fragments error: {0}")]
    Fragments(#[from] FragmentsError),
    #[error("Inode table error: {0}")]
    InodeTable(#[from] InodeTableError),
    #[error("Directory table error: {0}")]
    DirectoryTable(#[from] DirectoryTableError),
    #[error("Metadata error: {0}")]
    Metadata(#[from] MetadataError),
    #[error("Decompression error: {0}")]
    Decompress(#[from] DecompressError),
    #[error("Unsupported encoding")]
    Encoding,
    #[error("Invalid inode")]
    InvalidInode,
    #[cfg(feature = "memmap")]
    #[error("Failed to memory map file")]
    MemMap,
    #[error("{0}")]
    Fuse(#[from] ErrorFuse),
}

impl From<Error> for ErrorFuse {
    fn from(source: Error) -> Self {
        match source {
            Error::FileNotFound(_) | Error::DirectoryNotFound => Self::NoFileDir,
            Error::InvalidInode | Error::InvalidOffset => Self::InvalidArgument,
            Error::Encoding => Self::Unimplemented,
            Error::Fuse(e) => e,
            _ => Self::IO(source.to_string()),
        }
    }
}

/// Decompression error, for compressed archives.
#[derive(thiserror::Error, Debug)]
pub enum DecompressError {
    #[error("Failed to decompress data: {0}")]
    Io(#[from] tokio::io::Error),
    #[error("Unsupported compression {0:?}")]
    UnsupportedCompression(Compression),
}
/// Metadata parsing error.
#[derive(thiserror::Error, Debug)]
pub enum MetadataError {
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Invalid entry")]
    InvalidEntry,
    #[error("Invalid data length")]
    InvalidDataLength,
    #[error("Read failure")]
    ReadFailure(std::io::Error),
    #[error("Decompression error: {0}")]
    Decompress(#[from] DecompressError),
}
/// Inode table error.
#[derive(thiserror::Error, Debug)]
pub enum InodeTableError {
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Invalid entry")]
    InvalidEntry,
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(#[from] MetadataError),
    #[error("Read failure")]
    ReadFailure(std::io::Error),
}
/// Directory table error.
#[derive(thiserror::Error, Debug)]
pub enum DirectoryTableError {
    #[error("Invalid header")]
    InvalidHeader,
    #[error("Invalid entry")]
    InvalidEntry,
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(#[from] MetadataError),
    #[error("Read failure")]
    ReadFailure(std::io::Error),
}
/// Fragments error.
#[derive(thiserror::Error, Debug)]
pub enum FragmentsError {
    #[error("Invalid location in fragment table")]
    InvalidLocation,
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(#[from] MetadataError),
    #[error("Invalid fragment table entry")]
    InvalidEntry,
    #[error("Read failure")]
    ReadFailure(std::io::Error),
}
