pub mod atomic;
pub mod auth;
pub mod exit;
pub mod net;
pub mod net_async;
pub mod net_opt;
pub mod net_protocol;
pub mod shutdown;
pub mod tls;

pub const VERSION: &str = include_str!(concat!(
    env!("ABQ_WORKSPACE_DIR"),
    "build_artifact/abq_version.txt"
));
