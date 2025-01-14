use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use crate::modules::utils;

pub trait FileSystem {
    // Copies a file from `src` to `dest`.
    // Returns the number of bytes copied on success.
    fn copy_file<P: AsRef<Path>>(&self, src: P, dest: P) -> io::Result<u64>;

    // Moves (renames) a file from `src` to `dest`.
    // If `dest` is on a different file system, this method
    // should perform a copy and then remove the source file.
    fn move_file<P: AsRef<Path>>(&self, src: P, dest: P) -> io::Result<()>;

    // Extracts the file name (including extension) from the given path as a String.
    fn get_file_name<P: AsRef<Path>>(&self, path: P) -> io::Result<String>;

    // Extracts the file stem (excluding extension) from the given path as a String.
    fn get_file_stem<P: AsRef<Path>>(&self, path: P) -> io::Result<String>;

    // Extracts the extension from the given path as a String.
    fn get_file_extension<P: AsRef<Path>>(&self, path: P) -> io::Result<String>;

    // Joins a iterator tox `PathBuf`.
    fn join_path<S, I>(&self, paths: I) -> PathBuf
    where
        S: AsRef<Path>,
        I: IntoIterator<Item = S>;
}


pub struct LocalFileSystem;

impl LocalFileSystem {
    // Declare own methods
}

impl FileSystem for LocalFileSystem {
    fn copy_file<P: AsRef<Path>>(&self, src: P, dest: P) -> io::Result<u64> {
        fs::copy(src, dest)
    }

    fn move_file<P: AsRef<Path>>(&self, src: P, dest: P) -> io::Result<()> {
        fs::copy(&src, &dest)?;
        fs::remove_file(src)
    }

    fn get_file_name<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        utils::extract_path(path, Path::file_name, "file name")
    }

    fn get_file_stem<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        utils::extract_path(path, Path::file_stem, "file stem")
    }

    fn get_file_extension<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        utils::extract_path(path, Path::extension, "extension")
    }

    fn join_path<S, I>(&self, paths: I) -> PathBuf
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
