use std::{
    fmt, future::Future, net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4}, pin::Pin, sync::Arc, time::Duration,
};

use async_trait::async_trait;
use tokio::sync::oneshot;

use crate::{connectivity::hole_punch::port_mapping::{UdpPortMappingAttemptError, UdpPortMappingAttemptPhase, UdpPortMappingBackend}, events::{CoreEvent, CoreEventSink}};

const UPNP_RENEW_INTERVAL: Duration = Duration::from_secs(240);

pub(crate) trait PortMappingLease: Send + Sync + fmt::Debug {
    fn public_addr_resolved(&self, _mapped_addr: SocketAddr) {}
}

pub(crate) trait PortMappingInfo: Send + Sync + fmt::Debug {
    fn get_external_addr(&self) -> SocketAddrV4;
    fn get_local_addr(&self) -> SocketAddrV4;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortMappingBackend {
    Igd,
    NatPmp,
}

impl PortMappingBackend {
    pub fn name(self) -> &'static str {
        match self {
            Self::Igd => "igd",
            Self::NatPmp => "nat-pmp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortMappingAttemptPhase {
    Discovery,
    Establishment,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortMappingProtocol {
    Tcp,
    Udp,
}

#[derive(Debug)]
pub struct PortMappingAttemptError {
    phase: PortMappingAttemptPhase,
    source: anyhow::Error,
}

impl PortMappingAttemptError {
    pub fn discovery(source: impl Into<anyhow::Error>) -> Self {
        Self {
            phase: PortMappingAttemptPhase::Discovery,
            source: source.into(),
        }
    }

    pub fn establishment(source: impl Into<anyhow::Error>) -> Self {
        Self {
            phase: PortMappingAttemptPhase::Establishment,
            source: source.into(),
        }
    }

    pub(crate) fn phase(&self) -> PortMappingAttemptPhase {
        self.phase
    }

    pub(crate) fn source(&self) -> &anyhow::Error {
        &self.source
    }
}

impl fmt::Display for PortMappingAttemptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(f)
    }
}

impl std::error::Error for PortMappingAttemptError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

#[async_trait]
pub trait ActivePortMapping: Send + Sync + fmt::Debug {
    fn backend(&self) -> PortMappingBackend;

    fn protocol(&self) -> PortMappingProtocol;

    fn local_addr(&self) -> SocketAddr;

    fn gateway_external_port(&self) -> u16;

    async fn renew(&self) -> anyhow::Result<()>;

    async fn remove(&self) -> anyhow::Result<()>;
}

pub type PortMappingLifecycle = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

#[async_trait]
pub trait PortMappingPlatform: Send + Sync + 'static {
    async fn establish_port_mapping(
        &self,
        backend: PortMappingBackend,
        protocal: PortMappingProtocol,
        local_listener: &url::Url,
    ) -> Result<Box<dyn ActivePortMapping>, PortMappingAttemptError>;

    fn spawn_port_mapping_lifecycle(
        &self,
        _local_listener: url::Url,
        lifecycle: PortMappingLifecycle,
    ) {
        tokio::spawn(lifecycle);
    }

    async fn get_router_wan_ip(
        &self
    ) -> Result<IpAddr, anyhow::Error>;
}

pub(crate) struct ManagedPortMappingLease {
    events: Arc<dyn CoreEventSink>,
    local_listener: url::Url,
    protocal: PortMappingProtocol,
    backend: PortMappingBackend,
    gateway_external_port: u16,
    stop_tx: Option<oneshot::Sender<()>>,
}

impl fmt::Debug for ManagedPortMappingLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectPortMappingLease")
            .field("backend", &self.backend.name())
            .field("gateway_external_port", &self.gateway_external_port)
            .finish()
    }
}

impl Drop for ManagedPortMappingLease {
fn drop(&mut self) {
    if let Some(stop_tx) = self.stop_tx.take() {
            let _ = stop_tx.send(());
        }
    }
}

impl PortMappingLease for ManagedPortMappingLease {
    fn public_addr_resolved(&self, mapped_addr: SocketAddr) {
        self.events.emit(CoreEvent::UdpPortMappingEstablished {
            local_listener: self.local_listener.clone(),
            mapped_listener: udp_url(mapped_addr),
            backend: self.backend.name().to_owned(),
        });
        tracing::info!(
            local_listener = %self.local_listener,
            backend = self.backend.name(),
            gateway_external_port = self.gateway_external_port,
            stun_mapped_addr = %mapped_addr,
            "udp public addr resolved after port mapping"
        );
    }
}

pub(crate) async fn start_port_mapping(
    platform: Arc<dyn PortMappingPlatform>,
    events: Arc<dyn CoreEventSink>,
    protocal: PortMappingProtocol,
    local_listener: &url::Url,
) -> anyhow::Result<Option<Box<dyn PortMappingLease>>> {
    if !should_map_listener(local_listener) {
        return Ok(None);
    }

    let mapping = discover_port_mapping(platform.as_ref(), protocal, local_listener).await?;
    let backend = mapping.backend();
    let gateway_external_port = mapping.gateway_external_port();
    tracing::info!(
        %local_listener,
        backend = backend.name(),
        protocal = ?protocal,
        local_addr = %mapping.local_addr(),
        gateway_external_port,
        "port mapping established"
    );

    let (stop_tx, stop_rx) = oneshot::channel();
    platform.spawn_port_mapping_lifecycle(
        local_listener.clone(),
        Box::pin(run_port_mapping_lifecycle(
            local_listener.clone(),
            mapping,
            stop_rx,
        )),
    );

    Ok(Some(Box::new(ManagedPortMappingLease {
        events,
        local_listener: local_listener.clone(),
        backend,
        protocal,
        gateway_external_port,
        stop_tx: Some(stop_tx),
    })))
}

async fn discover_port_mapping(
    platform: &dyn PortMappingPlatform,
    protocal: PortMappingProtocol,
    local_listener: &url::Url,
) -> anyhow::Result<Box<dyn ActivePortMapping>> {
    let igd_error = match platform
        .establish_port_mapping(PortMappingBackend::Igd, protocal, local_listener)
        .await
    {
        Ok(mapping) => return Ok(mapping),
        Err(error) => error,
    };
    match igd_error.phase() {
        PortMappingAttemptPhase::Discovery => tracing::info!(
            igd_err = ?igd_error.source(),
            %local_listener,
            "igd gateway discovery failed, retry with nat-pmp"
        ),
        PortMappingAttemptPhase::Establishment => tracing::info!(
            igd_err = ?igd_error.source(),
            %local_listener,
            "igd Direct port mapping failed, retry with nat-pmp"
        ),
    }

    match platform
        .establish_port_mapping(PortMappingBackend::NatPmp, protocal, local_listener)
        .await
    {
        Ok(mapping) => Ok(mapping),
        Err(nat_pmp_error) => Err(combined_mapping_error(
            local_listener,
            igd_error,
            nat_pmp_error,
        )),
    }
}

fn combined_mapping_error(
    local_listener: &url::Url,
    igd_error: PortMappingAttemptError,
    nat_pmp_error: PortMappingAttemptError,
) -> anyhow::Error {
    let igd_label = match igd_error.phase() {
        PortMappingAttemptPhase::Discovery => "igd discovery error",
        PortMappingAttemptPhase::Establishment => "igd error",
    };
    let nat_pmp_label = match nat_pmp_error.phase() {
        PortMappingAttemptPhase::Discovery => "nat-pmp discovery error",
        PortMappingAttemptPhase::Establishment => "nat-pmp error",
    };
    anyhow::anyhow!(
        "udp port mapping failed for {local_listener}: {igd_label}: {}; {nat_pmp_label}: {}",
        igd_error.source(),
        nat_pmp_error.source(),
    )
}

async fn run_port_mapping_lifecycle(
    local_listener: url::Url,
    mapping: Box<dyn ActivePortMapping>,
    mut stop_rx: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = crate::foundation::time::sleep(UPNP_RENEW_INTERVAL) => {
                if let Err(error) = mapping.renew().await {
                    tracing::warn!(
                        err = ?error,
                        %local_listener,
                        backend = mapping.backend().name(),
                        gateway_external_port = mapping.gateway_external_port(),
                        "failed to renew udp port mapping"
                    );
                }
            }
            _ = &mut stop_rx => break,
        }
    }
    tracing::info!(%local_listener, "mytracing- ManagedDirectUdpPortMappingLease dropping");
    if let Err(error) = mapping.remove().await {
        tracing::debug!(
            err = ?error,
            %local_listener,
            backend = mapping.backend().name(),
            gateway_external_port = mapping.gateway_external_port(),
            "failed to remove udp port mapping"
        );
    }
}

pub(crate) fn should_map_listener(local_listener: &url::Url) -> bool {
    if local_listener.scheme() != "udp" || local_listener.scheme() != "tcp" {
        return false;
    }

    let Some(host) = listener_ipv4_host(local_listener) else {
        return false;
    };

    if host.is_loopback() || host.is_broadcast() {
        return false;
    }

    host.is_unspecified() || host.is_private() || host.is_link_local()
}

fn listener_ipv4_host(local_listener: &url::Url) -> Option<Ipv4Addr> {
    local_listener.host_str()?.parse().ok()
}

fn udp_url(addr: SocketAddr) -> url::Url {
    let mut url = url::Url::parse("udp://0.0.0.0").expect("static UDP URL should be valid");
    url.set_ip_host(addr.ip())
        .expect("socket IP should be a valid URL host");
    url.set_port(Some(addr.port()))
        .expect("UDP URL should accept a port");
    url
}

fn tcp_url(addr: SocketAddr) -> url::Url {
    let mut url = url::Url::parse("tcp://0.0.0.0").expect("static TCP URL should be valid");
    url.set_ip_host(addr.ip())
        .expect("socket IP should be a valid URL host");
    url.set_port(Some(addr.port()))
        .expect("TCP URL should accept a port");
    url
}
