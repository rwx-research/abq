use std::{env, fs, path::Path, process::Command};

fn version_from_git() -> String {
    let version_output = Command::new("git").args(["describe"]).output().unwrap();

    let git_described = String::from_utf8(version_output.stdout).unwrap();

    // ensure git describe output is valid
    assert!(
        git_described.starts_with("v1"),
        "tag ({}) should start with v1",
        git_described.trim()
    );

    // `describe` gives us `tag-revisions since-sha short`
    // Since right now `tag=init`, the initial commit,
    // transform this into `tag.revisions since.0+sha short`
    //
    // E.g. v1.0.0-1-gb0e5ce5
    //
    let mut parts = git_described[1..].splitn(4, '-');
    let latest_version_tag = parts.next().unwrap();

    // there is no describe additional when the current ref is tagged
    match (parts.next(), parts.next()) {
        (Some(commits_since), Some(short_sha)) => {
            // FYI short_sha is prefixed with g
            format!("{}-{}-{}", latest_version_tag, commits_since, short_sha)
        }
        _ => latest_version_tag.to_string(),
    }
}

/// Writes the currently-build ABQ version to $OUT_DIR/abq_version.txt
fn write_abq_version() {
    println!("cargo:rerun-if-changed=../../build_artifact");

    let version = env::var("NIX_ABQ_VERSION").unwrap_or_else(|_| version_from_git());

    let out_dir = Path::new(&env::var("ABQ_WORKSPACE_DIR").unwrap()).join("build_artifact/");
    if !out_dir.exists() {
        fs::create_dir(&out_dir).unwrap();
    }
    let dest_path = out_dir.join("abq_version.txt");

    // Avoid rewriting the version if it hasn't changed, to make local rebuilds faster.
    let should_rewrite = match fs::read_to_string(&dest_path) {
        Ok(old_version) => old_version.trim() != version.trim(),
        Err(_) => true,
    };
    if should_rewrite {
        fs::write(dest_path, version.trim()).unwrap();
    }
}

fn main() {
    write_abq_version();
}
