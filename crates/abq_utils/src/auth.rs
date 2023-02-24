//! Authentication and authorization of entities in ABQ, against an ABQ queue server.
//!
//! AuthN and AuthZ are collated via a [token][token::Token], with the
//! following behavior:
//!   - Entities authenticate and authorize by sending a [fixed-size][token::TOKEN_LEN] token
//!   - Authorization is determined by the content of a token and a given [auth
//!     strategy][ServerAuthStrategy].
//!   - An entity can be authorized as one of two roles:
//!     - User: this is the generic role permitting general use of the ABQ queue server; it is
//!       what ABQ workers exercise.
//!     - Admin: a role permitting administrative use of the ABQ queue server; never exercised by
//!       ABQ workers, but may be exercised by managers of queue servers.

mod token;

mod admin;
mod strategy;
mod user;

pub use strategy::Role;
pub use strategy::{Admin, User};

pub use admin::AdminToken;
pub use user::UserToken;

pub use strategy::{build_strategies, ClientAuthStrategy, ServerAuthStrategy};
