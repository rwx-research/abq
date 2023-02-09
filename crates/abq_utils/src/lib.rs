pub mod atomic;
pub mod auth;
pub mod exit;
pub mod log_assert;
pub mod net;
pub mod net_async;
pub mod net_opt;
pub mod net_protocol;
pub mod retry;
pub mod shutdown;
pub mod tls;
pub mod vec_map;

pub const VERSION: &str = include_str!(concat!(
    env!("ABQ_WORKSPACE_DIR"),
    "build_artifact/abq_version.txt"
));
