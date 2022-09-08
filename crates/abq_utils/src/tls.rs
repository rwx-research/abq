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
fn get_const_cert() -> tls::Certificate {
    const DEMO_SELF_SIGNED_SERVER_CERT: &[u8] = include_bytes!("../data/cert/server.crt");

    let mut cert_reader = io::BufReader::new(DEMO_SELF_SIGNED_SERVER_CERT);
    let mut certs = rustls_pemfile::certs(&mut cert_reader)
        .expect("invalid cert")
        .into_iter();
    tls::Certificate(certs.next().expect("invalid cert"))
}

const DEMO_SELF_SIGNED_SERVER_PRIVATE_KEY: &[u8] = include_bytes!("../data/cert/server.key");

/// Gets a constant ABQ server private key; for now all servers have the same key.
fn get_const_server_private_key() -> tls::PrivateKey {
    let mut key = io::BufReader::new(DEMO_SELF_SIGNED_SERVER_PRIVATE_KEY);
    let key = rustls_pemfile::pkcs8_private_keys(&mut key)
        .expect("invalid private key")
        .remove(0);
    tls::PrivateKey(key)
}

pub(crate) fn get_server_config() -> io::Result<Arc<tls::ServerConfig>> {
    let server_cert = get_const_cert();
    let server_key = get_const_server_private_key();

    let tls_config = tls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(vec![server_cert], server_key)
        .map_err(|e| std::io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Arc::new(tls_config))
}

pub(crate) fn get_client_config() -> io::Result<Arc<tls::ClientConfig>> {
    let server_cert = get_const_cert();

    let mut store = tls::RootCertStore::empty();
    store
        .add(&server_cert)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;

    let tls_config = tls::ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(store)
        .with_no_client_auth();
    Ok(Arc::new(tls_config))
}
