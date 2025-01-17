use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use crate::sa_helper;

pub struct SaLocalSystem;

impl SaLocalSystem {
    pub fn copy_file<P: AsRef<Path>>(&self, src: P, dest: P) -> io::Result<u64> {
        fs::copy(src, dest)
    }

    pub fn move_file<P: AsRef<Path>>(&self, src: P, dest: P) -> io::Result<()> {
        fs::copy(&src, &dest)?;
        fs::remove_file(src)
    }

    pub fn get_file_name<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        sa_helper::extract_path(path, Path::file_name, "file name")
    }

    pub fn get_file_stem<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        sa_helper::extract_path(path, Path::file_stem, "file stem")
    }

    pub fn get_file_extension<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        sa_helper::extract_path(path, Path::extension, "extension")
    }

    pub fn join_path<S, I>(&self, paths: I) -> PathBuf
    where
        S: AsRef<Path>,
        I: IntoIterator<Item = S>
    {
        let mut path_buf = PathBuf::new();
        for path in paths {
            path_buf.push(path.as_ref());
        }
        path_buf
    }
}