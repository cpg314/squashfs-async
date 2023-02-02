use std::path::{Path, PathBuf};
use std::sync::Once;

use rand::{Rng, SeedableRng};

pub const SPECS: [(&str, &[&str]); 3] = [
    ("nocomp", &["-noI", "-noId", "-noD", "-noF", "-noX"]),
    ("gzip", &["-comp", "gzip", "-Xcompression-level", "1"]),
    ("zstd", &["-comp", "zstd", "-Xcompression-level", "1"]),
];

pub fn tempdir() -> &'static Path {
    Path::new(env!("CARGO_TARGET_TMPDIR"))
}
pub fn filename(suffix: &str) -> PathBuf {
    tempdir().join(suffix).with_extension("squashfs")
}

static SETUP: Once = Once::new();

pub fn setup() {
    SETUP.call_once(|| {
        setup_impl().unwrap();
    });
}

const FILE_SIZE: usize = 20_000_000;
const FOLDERS: usize = 1;
const FILES_PER_FOLDER: usize = 8;

fn setup_impl() -> anyhow::Result<()> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    let contents = tempdir().join("contents");
    if SPECS
        .map(|(suffix, _)| filename(suffix))
        .iter()
        .any(|f| !f.exists())
    {
        if contents.exists() {
            std::fs::remove_dir_all(&contents)?;
        }
        std::fs::create_dir_all(&contents)?;
        println!("Creating random files");
        random_files(FILES_PER_FOLDER, &contents, FILE_SIZE, &mut rng)?;
        for i in 0..FOLDERS {
            let dir = contents.join(i.to_string());
            std::fs::create_dir_all(&dir)?;
            random_files(FILES_PER_FOLDER, &dir, FILE_SIZE, &mut rng)?;
        }

        println!("Creating test squashfs");
        for (suffix, options) in SPECS {
            let filename = &filename(suffix);
            if !filename.exists() {
                mksquashfs(&contents, filename, options)?;
            }
        }
        println!("Cleaning up random files");
        std::fs::remove_dir_all(&contents)?;
    } else {
        println!("Test data already generated",);
    }
    Ok(())
}

fn random_file(path: &Path, size: usize, rng: &mut impl Rng) -> anyhow::Result<()> {
    let f = std::fs::File::create(path)?;
    let mut f = std::io::BufWriter::new(f);
    let reader = rng
        .sample_iter::<u8, _>(rand::distributions::Standard)
        .take(size);
    let mut reader = iter_read::IterRead::new(reader);
    std::io::copy(&mut reader, &mut f)?;
    Ok(())
}

fn random_files(n: usize, path: &Path, size: usize, rng: &mut impl Rng) -> anyhow::Result<()> {
    for i in 0..n {
        random_file(&path.join(format!("file-{}.random", i)), size, rng)?;
    }
    Ok(())
}
fn mksquashfs<I, S>(input: &Path, dest: &Path, options: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let cmd = std::process::Command::new("mksquashfs")
        .args([input, dest])
        .args(["-mkfs-time", "0", "-reproducible"])
        .args(options)
        .output()?;
    if !cmd.status.success() {
        anyhow::bail!(
            "Failed to run mksquashfs: {}",
            std::str::from_utf8(&cmd.stderr)?
        );
    }
    Ok(())
}
