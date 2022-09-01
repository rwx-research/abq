use std::net::{SocketAddr, TcpStream};

use abq_utils::net_protocol::{self, entity::EntityId, queue};

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

    pub fn is_healthy(&self) -> bool {
        macro_rules! bail {
            ($e:expr) => {
                match $e {
                    Ok(v) => v,
                    Err(_) => return false,
                }
            };
        }

        let entity = EntityId::new();

        tracing::debug!(?entity, "starting healthchecks");

        let mut conn = bail!(TcpStream::connect(self.addr()));
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
                    net_protocol::work_server::WorkServerRequest::HealthCheck,
                ));
            }
            HealthCheckKind::Negotiator(_) => {
                bail!(net_protocol::write(
                    &mut conn,
                    abq_workers::negotiate::MessageToQueueNegotiator::HealthCheck,
                ));
            }
        }

        let health_msg: net_protocol::health::HEALTH = bail!(net_protocol::read(&mut conn));
        health_msg == net_protocol::health::HEALTHY
    }
}
