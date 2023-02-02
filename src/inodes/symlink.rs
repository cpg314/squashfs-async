use serde::Deserialize;

use super::super::error::InodeTableError;
use crate::deser;

#[derive(Debug, Default, Deserialize)]
pub struct Symlink {
    #[allow(dead_code)]
    link_count: u32,
    target_size: u32,
    #[serde(skip)]
    target: String,
}
impl Symlink {
    pub async fn from_reader(mut r: impl crate::AsyncRead) -> Result<Self, InodeTableError> {
        let mut link: Self = deser::bincode_deser_from(&mut r, 8)
            .await
            .map_err(|_| InodeTableError::InvalidEntry)?;
        link.target = deser::bincode_deser_string_from(r, link.target_size as usize)
            .await
            .map_err(|_| InodeTableError::InvalidEntry)?;
        Ok(link)
    }
}
