use std::net::SocketAddr;

use abq_utils::net_protocol::{self, entity::Entity, health::Health, queue, work_server};

type ClientOptions = abq_utils::net_opt::ClientOptions<abq_utils::auth::User>;

pub(crate) enum HealthCheckKind {
    Queue(SocketAddr),
    WorkScheduler(SocketAddr),
    Negotiator(SocketAddr),
}

impl std::fmt::Display for HealthCheckKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthCheckKind::Queue(addr) => {
                write!(f, "Queue at {}", addr)
            }
            HealthCheckKind::WorkScheduler(addr) => {
                write!(f, "Work scheduler at {}", addr)
            }
            HealthCheckKind::Negotiator(addr) => {
                write!(f, "Negotiator at {}", addr)
            }
        }
    }
}

impl HealthCheckKind {
    fn addr(&self) -> SocketAddr {
        match self {
            HealthCheckKind::Queue(addr)
            | HealthCheckKind::WorkScheduler(addr)
            | HealthCheckKind::Negotiator(addr) => *addr,
        }
    }

    pub fn get_health(&self, client_opts: ClientOptions) -> Option<Health> {
        macro_rules! bail {
            ($e:expr) => {
                match $e {
                    Ok(v) => v,
                    Err(_) => return None,
                }
            };
        }

        let entity = Entity::local_client();

        tracing::debug!(?entity, "starting healthchecks");

        let client = client_opts.build().unwrap();
        let mut conn = bail!(client.connect(self.addr()));
        match self {
            HealthCheckKind::Queue(_) => {
                bail!(net_protocol::write(
                    &mut conn,
                    queue::Request {
                        entity,
                        message: queue::Message::HealthCheck,
                    }
                ));
            }
            HealthCheckKind::WorkScheduler(_) => {
                bail!(net_protocol::write(
                    &mut conn,
                    work_server::Request {
                        entity,
                        message: work_server::Message::HealthCheck,
                    }
                ));
            }
            HealthCheckKind::Negotiator(_) => {
                bail!(net_protocol::write(
                    &mut conn,
                    abq_workers::negotiate::Request {
                        entity,
                        message: abq_workers::negotiate::MessageToQueueNegotiator::HealthCheck,
                    }
                ));
            }
        }

        let health_msg: net_protocol::health::Health = bail!(net_protocol::read(&mut conn));
        Some(health_msg)
    }
}
