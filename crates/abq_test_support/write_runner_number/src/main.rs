use std::io::Write;

use fs4::FileExt;

fn main() {
    let fi = std::env::args().nth(1).unwrap();
    let mut fi = std::fs::OpenOptions::new().append(true).open(fi).unwrap();

    fi.lock_exclusive().unwrap();

    writeln!(fi, "{}", std::env::var("ABQ_RUNNER").unwrap()).unwrap();
    std::process::exit(1);
}
