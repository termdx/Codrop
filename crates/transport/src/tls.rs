//! QUIC endpoints and TLS plumbing.
//!
//! QUIC mandates TLS. For this LAN prototype we use a self-signed cert and a client that
//! skips certificate verification — fine for a trusted local network, but a real deployment
//! must pin per-device certificates (device pairing) so peers authenticate each other.

use anyhow::Result;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Endpoint, ServerConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use std::net::SocketAddr;
use std::sync::{Arc, Once};

static INIT: Once = Once::new();

/// rustls needs a process-wide crypto provider installed before building configs.
fn install_provider() {
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// A QUIC server endpoint bound to `bind`, using a fresh self-signed cert.
pub fn server_endpoint(bind: SocketAddr) -> Result<Endpoint> {
    install_provider();
    Ok(Endpoint::server(server_config()?, bind)?)
}

/// A QUIC client endpoint on an ephemeral port that trusts any server (LAN prototype).
pub fn client_endpoint() -> Result<Endpoint> {
    install_provider();
    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;
    endpoint.set_default_client_config(client_config()?);
    Ok(endpoint)
}

fn server_config() -> Result<ServerConfig> {
    let certified = rcgen::generate_simple_self_signed(vec!["codrop".to_string()])?;
    let cert_der: CertificateDer = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der()));
    Ok(ServerConfig::with_single_cert(vec![cert_der], key)?)
}

fn client_config() -> Result<ClientConfig> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    Ok(ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?)))
}

/// Accept any server certificate. DANGEROUS — prototype only; replace with device pinning.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
