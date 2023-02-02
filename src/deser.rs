// We use bincode for deserializing the squashfs internal structures.
use bincode::Options;
use tokio::io::{AsyncRead, AsyncReadExt};

pub fn bincode_deser<T>(bytes: &[u8]) -> bincode::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .reject_trailing_bytes()
        .with_limit(1000)
        .deserialize(bytes)
}
pub async fn bincode_deser_from<T>(mut r: impl AsyncRead + Unpin, size: usize) -> bincode::Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let mut buf = vec![0; size];
    r.read_exact(&mut buf).await?;
    bincode_deser(&buf)
}

pub async fn bincode_deser_string_from(
    mut r: impl AsyncRead + Unpin,
    size: usize,
) -> bincode::Result<String> {
    let mut buf = vec![0; size];
    r.read_exact(&mut buf).await?;
    Ok(std::str::from_utf8(&buf)
        .map_err(bincode::ErrorKind::InvalidUtf8Encoding)?
        .to_string())
}

macro_rules! from_reader {
    ($t:ty,$size:literal) => {
        impl $t {
            pub async fn from_reader(mut r: impl crate::AsyncRead) -> Result<Self, bincode::Error> {
                super::deser::bincode_deser_from(&mut r, $size).await
            }
        }
    };
}

pub(crate) use from_reader;
