//! Native UDP hole-punch platform adapter.
//!
//! Peer selection, signaling, RPC registration, socket/session ownership and
//! lifecycle live in `easytier-core`. Native only supplies OS port mapping.

use std::{fmt, net::{IpAddr, SocketAddr}, sync::Arc};

use async_trait::async_trait;
use easytier_core::connectivity::{
    port_mapping::{ ActivePortMapping, PortMappingBackend, PortMappingProtocol },
    hole_punch::port_mapping::{
        ActiveUdpPortMapping, UdpPortMappingAttemptError, UdpPortMappingBackend,
        UdpPortMappingLifecycle, UdpPortMappingPlatform, udp_port_mapping_backend, udp_port_mapping_err
    }
};

use crate::common::{netns::NetNS, upnp};

struct RuntimeUdpHolePunchPlatform {
    net_ns: NetNS,
}

struct ActiveUdpPortMappingProxy {
    proxy: Box<dyn ActivePortMapping>
}

impl fmt::Debug for ActiveUdpPortMappingProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.proxy.fmt(f)
    }
}
#[async_trait]
impl ActiveUdpPortMapping for ActiveUdpPortMappingProxy {
    fn backend(&self) -> UdpPortMappingBackend {
        match self.proxy.backend() {
            PortMappingBackend::Igd => UdpPortMappingBackend::Igd,
            PortMappingBackend::NatPmp => UdpPortMappingBackend::NatPmp,
        }
    }

    fn local_addr(&self) -> SocketAddr {
        self.proxy.local_addr()
    }

    fn gateway_external_port(&self) -> u16 {
        self.proxy.gateway_external_port()
    }

    async fn renew(&self) -> anyhow::Result<()> {
        self.proxy.renew().await
    }

    async fn remove(&self) -> anyhow::Result<()> {
        self.proxy.remove().await
    }
}

fn active_udp_port_mapping(proxy: Box<dyn ActivePortMapping>) -> Box<dyn ActiveUdpPortMapping> {
    Box::new(ActiveUdpPortMappingProxy { proxy })
}

#[async_trait]
impl UdpPortMappingPlatform for RuntimeUdpHolePunchPlatform {
    async fn establish_udp_port_mapping(
        &self,
        backend: UdpPortMappingBackend,
        local_listener: &url::Url,
    ) -> Result<Box<dyn ActiveUdpPortMapping>, UdpPortMappingAttemptError> {
        upnp::establish_port_mapping(self.net_ns.clone(), udp_port_mapping_backend(&backend), PortMappingProtocol::Udp, local_listener.clone()).await
        .map(active_udp_port_mapping).map_err(udp_port_mapping_err)
    }

    fn spawn_udp_port_mapping_lifecycle(
        &self,
        local_listener: url::Url,
        lifecycle: UdpPortMappingLifecycle,
    ) {
        upnp::spawn_udp_port_mapping_lifecycle(self.net_ns.clone(), local_listener, lifecycle);
    }
    async fn get_router_wan_ip(
        &self,
    ) -> Result<IpAddr, anyhow::Error> {
        anyhow::bail!("unimplemented");
    }
}

pub(crate) fn runtime_udp_hole_punch_platform(net_ns: NetNS) -> Arc<dyn UdpPortMappingPlatform> {
    Arc::new(RuntimeUdpHolePunchPlatform { net_ns })
}
