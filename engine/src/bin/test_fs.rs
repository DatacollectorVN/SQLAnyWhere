use engine::modules::fs::{FileSystem, LocalFileSystem};
use std::path::PathBuf;
use std::io::Error;
fn main() {
    let fs: LocalFileSystem = LocalFileSystem;
    let mut source_file: &str = "src/data/test_fs/f1/data.csv";
    let mut dest_file: &str = "src/data/test_fs/f2/data_1.csv";

    let res: Result<u64, Error> = fs.copy_file(source_file, dest_file);
    println!("Complete copy with status: {:?}", res);

    source_file = "src/data/test_fs/f2/data_1.csv";
    dest_file = "src/data/test_fs/f2/data_2.csv";

    let res: Result<(), Error> = fs.move_file(source_file, dest_file);
    println!("Complete move with status: {:?}", res);

    let file_name: Result<String, Error> = fs.get_file_name(source_file);
    println!("file_name: {:?}", file_name);

    let file_stem: Result<String, Error> = fs.get_file_stem(source_file);
    println!("file_stem: {:?}", file_stem);

    let file_extension: Result<String, Error> = fs.get_file_extension(source_file);
    println!("file_extension: {:?}", file_extension);

    let paths: Vec<&str> = vec!["src", "data", "test_fs", "f2", "data_1.csv"];
    let full_path: PathBuf = fs.join_path(paths);

    println!("full_path: {:?}", full_path);

}