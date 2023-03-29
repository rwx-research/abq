pub mod atomic;
pub mod auth;
pub mod capture_output;
pub mod decay;
pub mod error;
pub mod exit;
pub mod log_assert;
pub mod net;
pub mod net_async;
pub mod net_opt;
pub mod net_protocol;
pub mod oneshot_notify;
pub mod results_handler;
pub mod retry;
pub mod server_shutdown;
pub mod time;
pub mod timeout_future;
pub mod tls;
pub mod vec_map;
pub mod whitespace;

pub const VERSION: &str = include_str!(concat!(
    env!("ABQ_WORKSPACE_DIR"),
    "build_artifact/abq_version.txt"
));
