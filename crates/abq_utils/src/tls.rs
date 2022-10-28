//! Utilities for managing TLS configurations.
//!
//! For now, we suppose that all ABQ servers have the same DNS name.
//! Moreover, we issue a constant, self-signed certificate and private key for them.
//! Clients should authenticate only against that certificate.

use std::{io, sync::Arc};

use rustls as tls;

/// Gets a constant ABQ server DNS name; for now all servers have the same name.
pub(crate) fn get_server_name() -> tls::ServerName {
    tls::ServerName::try_from("abq.rwx").expect("this should be a legal name")
}

/// Gets a constant ABQ server cert; for now all servers have the same cert.
fn cert_of_bytes(bytes: &[u8]) -> io::Result<tls::Certificate> {
    let mut cert_reader = io::BufReader::new(bytes);
    let mut certs = rustls_pemfile::certs(&mut cert_reader)?.into_iter();

    let cert = certs
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS certificate not found"))?;

    Ok(tls::Certificate(cert))
}

/// Gets a constant ABQ server private key; for now all servers have the same key.
fn pkey_of_bytes(bytes: &[u8]) -> io::Result<tls::PrivateKey> {
    let mut key = io::BufReader::new(bytes);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key)?.into_iter();

    let key = keys
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TLS private key not found"))?;

    Ok(tls::PrivateKey(key))
}

#[derive(Clone)]
pub(crate) enum ServerTlsStrategyInner {
    NoTls,
    Config(Arc<tls::ServerConfig>),
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ServerTlsStrategy(pub(crate) ServerTlsStrategyInner);

impl ServerTlsStrategy {
    pub const fn no_tls() -> Self {
        Self(ServerTlsStrategyInner::NoTls)
    }

    pub fn from_cert(cert_bytes: &[u8], pkey_bytes: &[u8]) -> io::Result<Self> {
        let server_cert = cert_of_bytes(cert_bytes)?;
        let server_key = pkey_of_bytes(pkey_bytes)?;

        let tls_config = tls::ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(vec![server_cert], server_key)
            .map_err(|e| std::io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        Ok(Self(ServerTlsStrategyInner::Config(Arc::new(tls_config))))
    }
}

#[derive(Clone)]
pub(crate) enum ClientTlsStrategyInner {
    NoTls,
    Config(Arc<tls::ClientConfig>),
}

#[derive(Clone)]
#[repr(transparent)]
pub struct ClientTlsStrategy(pub(crate) ClientTlsStrategyInner);

impl ClientTlsStrategy {
    pub const fn no_tls() -> Self {
        Self(ClientTlsStrategyInner::NoTls)
    }

    pub fn from_cert(cert_bytes: &[u8]) -> io::Result<Self> {
        let server_cert = cert_of_bytes(cert_bytes)?;

        let mut store = tls::RootCertStore::empty();
        store
            .add(&server_cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

        let tls_config = tls::ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(store)
            .with_no_client_auth();

        Ok(Self(ClientTlsStrategyInner::Config(Arc::new(tls_config))))
    }
}
