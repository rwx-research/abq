use std::{env, fs, path::Path, process::Command};

/// Writes the currently-build ABQ version to $OUT_DIR/abq_version.txt
fn write_abq_version() {
    let version_output = Command::new("git")
        .args(&["describe", "--dirty"])
        .output()
        .unwrap();
    let version = String::from_utf8(version_output.stdout).unwrap();

    let encryption = if cfg!(feature = "tls") { "TLS" } else { "none" };

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("abq_version.txt");
    eprintln!("{}", out_dir);
    fs::write(
        dest_path,
        format!("{} (encryption: {})", version.trim(), encryption),
    )
    .unwrap();
}

fn main() {
    write_abq_version();
}
