use std::path::{Path};
use std::io::{self, Error, ErrorKind};
use std::ffi::OsStr;

pub fn extract_path<P, F>(
    path: P,
    extract_path_fn: F,
    mode: &str
) -> io::Result<String>
where
    P: AsRef<Path>,
    F: Fn(&Path) -> Option<&OsStr>
{
    let path_ref: &Path = path.as_ref();
    match extract_path_fn(path_ref) {
        Some(os_str) => os_str
            .to_str()
            .map(|s| s.to_string())
            .ok_or_else(
                || {
                    Error::new(ErrorKind::InvalidData, format!("Invalid UTF-8 in {}", mode),)
                }
            ),
        None => Err(Error::new(ErrorKind::InvalidData, format!("Path has no valid {}", mode)))
    }
}