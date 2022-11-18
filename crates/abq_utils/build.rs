use std::{env, fs, path::Path, process::Command};

fn version_from_git() -> String {
    let version_output = Command::new("git")
        .args(["describe", "--dirty"])
        .output()
        .unwrap();

    let git_described = String::from_utf8(version_output.stdout).unwrap();

    // `describe` gives us `tag-revisions since-sha short`
    // Since right now `tag=init`, the initial commit,
    // transform this into `tag.revisions since.0+sha short`
    //
    // E.g. init-182-ga7a3202 => 0.182.0+ga7a3202
    //
    // Panic so that once we have named versions, we know to generate proper names here.
    let mut parts = git_described.splitn(3, '-');
    assert_eq!(parts.next().unwrap(), "init", "{git_described}");
    let commits_since = parts.next().unwrap();
    let short_sha = parts.next().unwrap();

    format!("0.{commits_since}.0+{short_sha}")
}

/// Writes the currently-build ABQ version to $OUT_DIR/abq_version.txt
fn write_abq_version() {
    println!("cargo:rerun-if-changed=build_artifact/");

    let version = env::var("NIX_ABQ_VERSION").unwrap_or_else(|_| version_from_git());

    let out_dir = Path::new(&env::var("ABQ_WORKSPACE_DIR").unwrap()).join("build_artifact/");
    if !out_dir.exists() {
        fs::create_dir(&out_dir).unwrap();
    }
    let dest_path = out_dir.join("abq_version.txt");
    fs::write(dest_path, version.trim()).unwrap();
}

fn main() {
    write_abq_version();
}
