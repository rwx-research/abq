use std::path::PathBuf;

pub const WORKSPACE: &str = env!("ABQ_WORKSPACE_DIR");

pub fn artifacts_dir() -> PathBuf {
    let path = if cfg!(all(target_arch = "x86_64", target_env = "musl")) {
        // GHA is using a musl target
        "target/x86_64-unknown-linux-musl/release"
    } else if cfg!(debug_assertions) {
        "target/debug"
    } else {
        "target/release"
    };
    PathBuf::from(WORKSPACE).join(path)
}
