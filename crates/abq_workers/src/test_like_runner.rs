//! Trivial runners only used for testing.

pub fn echo(input: String) -> String {
    input
}

pub struct ExecWork {
    pub cmd: String,
    pub args: Vec<String>,
}

pub fn exec(input: ExecWork) -> String {
    let output = std::process::Command::new(input.cmd)
        .args(input.args)
        .output()
        .unwrap();

    let success = output.status.success();
    let message = if success {
        output.stdout
    } else {
        output.stderr
    };
    String::from_utf8(message).unwrap()
}
