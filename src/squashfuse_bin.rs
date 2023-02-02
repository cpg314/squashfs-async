use std::path::{Path, PathBuf};
use std::process;

use clap::Parser;
use fuser_async::{FilesystemFUSE, FilesystemSSUS};
use tracing::*;

use squashfs_async::{pools::LocalBackend, Options, SquashFs};

#[derive(Parser)]
#[clap(name = "squashfuse-rs")]
struct Flags {
    /// Input squashfs image
    input: PathBuf,
    /// Mountpoint
    mountpoint: PathBuf,
    #[clap(flatten)]
    options: Options,
    #[clap(long, arg_enum, default_value_t = if cfg!(feature="memmap") {LocalBackend::MemMap} else { LocalBackend::Tokio })]
    backend: LocalBackend,
    #[clap(long, short)]
    debug: bool,
}

async fn mount<F: FilesystemSSUS + Send + Sync>(fs: F, mountpoint: &Path) -> anyhow::Result<()>
where
    F::Error: std::fmt::Display,
{
    let fuse = FilesystemFUSE::new(fs);

    let _mount = fuser::spawn_mount2(
        fuse,
        mountpoint,
        &[fuser::MountOption::RO, fuser::MountOption::Async],
    )?;
    tokio::signal::ctrl_c().await?;
    Ok(())
}
macro_rules! backend_variant {
    ($t:path, $args:ident) => {
        mount(
            SquashFs::<$t>::open(&$args.input, &$args.options).await?,
            &$args.mountpoint,
        )
        .await?
    };
}

async fn main_impl(args: Flags) -> anyhow::Result<()> {
    squashfs_async::utils::setup_logger(args.debug)?;
    info!("Mounting {:?} at {:?}", args.input, args.mountpoint);
    match args.backend {
        LocalBackend::Tokio => backend_variant!(squashfs_async::pools::LocalReadersPoolTokio, args),
        #[cfg(feature = "asyncfs")]
        LocalBackend::AsyncFs => {
            backend_variant!(squashfs_async::pools::LocalReadersPoolAsyncFs, args)
        }
        #[cfg(feature = "memmap")]
        LocalBackend::MemMap => {
            backend_variant!(squashfs_async::pools::LocalReadersPoolMemMap, args)
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    let args = Flags::parse();
    if let Err(e) = main_impl(args).await {
        error!("{:?}", e);
        process::exit(1)
    }
}
