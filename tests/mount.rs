use std::path::Path;
use std::process::{Child, Command};

use clap::ArgEnum;

use squashfs_async::pools::LocalBackend;

pub trait Mount {
    fn name(&self) -> String;
    fn mount(&mut self, source: &Path, dest: &Path) -> anyhow::Result<()>;
    fn handle(&mut self) -> &mut Child;
    fn unmount(&mut self) -> anyhow::Result<()> {
        let handle = self.handle();
        let mut kill = Command::new("kill")
            .args(["-s", "INT", &handle.id().to_string()])
            .spawn()?;
        kill.wait()?;
        let status = handle.wait()?;
        anyhow::ensure!(status.success());
        Ok(())
    }
}
#[derive(Default)]
pub struct Squashfuse {
    command: String,
    handle: Option<std::process::Child>,
}
impl Squashfuse {
    pub fn new(command: &str) -> Self {
        Self {
            command: command.into(),
            handle: None,
        }
    }
}
impl Mount for Squashfuse {
    fn name(&self) -> String {
        self.command.clone()
    }
    fn mount(&mut self, source: &Path, dest: &Path) -> anyhow::Result<()> {
        self.handle = Some(
            Command::new(&self.command)
                .arg("-f")
                .args([&source, &dest])
                .spawn()?,
        );
        Ok(())
    }
    fn handle(&mut self) -> &mut Child {
        self.handle.as_mut().unwrap()
    }
}
pub struct SquashfuseRs {
    handle: Option<std::process::Child>,
    backend: LocalBackend,
}
impl From<LocalBackend> for SquashfuseRs {
    fn from(backend: LocalBackend) -> Self {
        Self {
            handle: None,
            backend,
        }
    }
}
impl Mount for SquashfuseRs {
    fn name(&self) -> String {
        format!("squashfuse-rs-{:?}", self.backend)
    }
    fn mount(&mut self, source: &Path, dest: &Path) -> anyhow::Result<()> {
        self.handle = Some(
            Command::new(env!("CARGO_BIN_EXE_squashfuse-rs"))
                .args([&source, &dest])
                .args(["--cache-mb", "100"])
                .args(if cfg!(feature = "memmap") {
                    vec![
                        "--backend",
                        self.backend.to_possible_value().unwrap().get_name(),
                    ]
                } else {
                    vec![]
                })
                .stdout(std::process::Stdio::null())
                .spawn()?,
        );
        Ok(())
    }
    fn handle(&mut self) -> &mut Child {
        self.handle.as_mut().unwrap()
    }
}
