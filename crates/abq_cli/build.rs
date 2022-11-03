use std::{env, fs, path::Path, process::Command};

/// Writes the currently-build ABQ version to $OUT_DIR/abq_version.txt
fn write_abq_version() {
    let version_output = Command::new("git")
        .args(["describe", "--dirty"])
        .output()
        .unwrap();
    let version = String::from_utf8(version_output.stdout).unwrap();

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("abq_version.txt");
    fs::write(dest_path, version.trim()).unwrap();
}

fn main() {
    write_abq_version();
}
