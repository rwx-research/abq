use std::path::PathBuf;

pub const WORKSPACE: &str = env!("ABQ_WORKSPACE_DIR");

pub fn artifacts_dir() -> PathBuf {
    let path = if cfg!(all(target_arch = "x86_64", target_env = "musl")) {
        // GHA is using a musl target
        if cfg!(debug_assertions) {
            "target/x86_64-unknown-linux-musl/release-unstable"
        } else {
            "target/x86_64-unknown-linux-musl/release"
        }
    } else if cfg!(debug_assertions) {
        "target/debug"
    } else {
        "target/release"
    };
    PathBuf::from(WORKSPACE).join(path)
}

/// Only relevant with [traced_test][tracing_test::traced_test].
#[track_caller]
pub fn assert_scoped_log(scope: &str, log: &str) {
    assert_scoped_logs(scope, |logs| logs.iter().any(|l| l.contains(log)));
}

/// Only relevant with [traced_test][tracing_test::traced_test].
#[track_caller]
pub fn assert_scoped_logs(scope: &str, f: impl Fn(&[&str]) -> bool) {
    match tracing_test::internal::logs_assert(scope, |logs| {
        if f(logs) {
            Ok(())
        } else {
            Err("logs not found".to_owned())
        }
    }) {
        Ok(()) => (),
        Err(e) => panic!("{e}"),
    }
}

pub fn sanitize_output(s: &str) -> String {
    let re_paths = regex::Regex::new(r"at crates.*").unwrap();
    let re_simulation_path = regex::Regex::new(r".*abqtest_native_runner_simulation.*").unwrap();
    let re_finished_seconds = regex::Regex::new(r"Finished in .*").unwrap();

    let s = re_paths.replace_all(s, "at <stripped path>");
    let s = re_simulation_path.replace(&s, "<simulation cmd>");
    let s =
        re_finished_seconds.replace(&s, "Finished in XX seconds (XX seconds spent in test code)");

    s.into_owned()
}
