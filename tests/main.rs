mod mount;
mod testdata;

use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use squashfs_async::pools::LocalBackend;
use squashfs_async::utils::MeanStd;

// See https://stackoverflow.com/a/48534068
struct HashWriter<T: Hasher>(T);
impl<T: Hasher> std::io::Write for HashWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.write(buf);
        Ok(buf.len())
    }
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.write(buf).map(|_| ())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn process_folder(folder: &Path, n_chunks: i32, hash: bool) -> anyhow::Result<u64> {
    let files: Vec<PathBuf> = glob::glob(folder.join("**/*").to_str().unwrap())?
        .filter_map(|f| f.ok())
        .filter(|f| f.is_file())
        .collect();
    let mut tasks = vec![];
    for chunk in files.chunks((files.len() as f64 / n_chunks as f64).ceil() as usize) {
        let chunk = chunk.to_vec();
        tasks.push(std::thread::spawn(move || {
            let hasher = rustc_hash::FxHasher::default();
            let mut hasher = HashWriter(hasher);
            for f in chunk {
                let f = std::fs::File::open(f)?;
                let mut f = std::io::BufReader::with_capacity(131072, f);
                let mut sink = std::io::sink();
                if hash {
                    std::io::copy(&mut f, &mut hasher).unwrap();
                } else {
                    std::io::copy(&mut f, &mut sink)?;
                }
            }
            anyhow::Ok(hasher.0.finish())
        }));
    }
    let mut hasher = rustc_hash::FxHasher::default();
    for t in tasks {
        t.join().unwrap().unwrap().hash(&mut hasher);
    }
    Ok(hasher.finish())
}

fn test_one(
    suffix: &str,
    mut mount: Box<dyn mount::Mount>,
    mountpoint: &Path,
    n_chunks: i32,
    runs: usize,
) -> anyhow::Result<(u64, MeanStd, u64)> {
    let mut durations = vec![];
    let filename = testdata::filename(suffix);
    let mut hashes = BTreeSet::<u64>::default();
    for _ in 0..runs {
        if let Err(e) = procfs::sys::vm::drop_caches(procfs::sys::vm::DropCache::All) {
            eprintln!(
                "Failed to drop caches {}, run with sudo. Continuing nevertheless",
                e
            );
        }
        std::thread::sleep(std::time::Duration::from_secs(2));

        mount.mount(&filename, mountpoint)?;
        std::thread::sleep(std::time::Duration::from_secs(2));

        let start = std::time::Instant::now();
        process_folder(mountpoint, n_chunks, false)?;
        durations.push(start.elapsed().as_millis() as f64);
        hashes.insert(process_folder(mountpoint, n_chunks, true)?);
        mount.unmount()?;
    }
    assert_eq!(hashes.len(), 1);
    let hash = hashes.into_iter().next().unwrap();
    let durations: MeanStd = durations.into_iter().collect();
    let filesize = std::fs::metadata(&filename)?.len();
    println!(
        "{:.0} ms ({:.1} MB/s, {})",
        durations,
        filesize as f64 / 1e6 / (durations.mean / 1000.0),
        hash
    );
    Ok((hash, durations, filesize))
}

#[derive(serde::Serialize)]
struct TestResult {
    mount_name: String,
    n_chunks: i32,
    duration_ms: MeanStd,
    filesize: u64,
    spec: &'static str,
}

#[test]
fn test() -> anyhow::Result<()> {
    testdata::setup();
    let runs: usize = if let Some(runs) = std::env::var_os("N_RUNS") {
        runs.to_str().unwrap().parse()?
    } else {
        1
    };
    println!("Running tests with {} runs. Set N_RUNS to change.", runs);
    let mountpoint = testdata::tempdir().join("mountpoint");
    if !mountpoint.exists() {
        std::fs::create_dir_all(&mountpoint)?;
    }

    let mut results = vec![];

    for n_chunks in [4, 1] {
        println!("Testing with {} chunks\n", n_chunks);
        let mut hashes = BTreeSet::<u64>::default();
        for (spec, _) in testdata::SPECS {
            let mut duration: Option<f64> = None;
            println!("Testing {}\n", spec);
            let mounts: Vec<Box<dyn mount::Mount>> = vec![
                Box::new(mount::Squashfuse::new("squashfuse")),
                // Box::new(mount::Squashfuse::new("squashfuse_ll")),
                Box::new(mount::SquashfuseRs::from(LocalBackend::Tokio)),
                #[cfg(feature = "asyncfs")]
                Box::new(mount::SquashfuseRs::from(LocalBackend::AsyncFs)),
                #[cfg(feature = "memmap")]
                Box::new(mount::SquashfuseRs::from(LocalBackend::MemMap)),
            ];
            for mount in mounts {
                let mount_name = mount.name();
                println!("{}", mount_name);
                let (hash, this_duration, filesize) =
                    test_one(spec, mount, &mountpoint, n_chunks, runs)?;
                hashes.insert(hash);
                if let Some(duration) = duration {
                    println!("{:.2}", this_duration.mean / duration);
                } else {
                    duration = Some(this_duration.mean);
                }
                results.push(TestResult {
                    mount_name,
                    n_chunks,
                    spec,
                    filesize,
                    duration_ms: this_duration,
                });
            }
            print!("\n\n",);
        }
        assert_eq!(hashes.len(), 1);
    }
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}
