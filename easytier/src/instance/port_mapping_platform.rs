//! Native UDP hole-punch platform adapter.
//!
//! Peer selection, signaling, RPC registration, socket/session ownership and
//! lifecycle live in `easytier-core`. Native only supplies OS port mapping.

use std::{net::Ipv4Addr, sync::Arc};

use async_trait::async_trait;
use easytier_core::connectivity::port_mapping::{
    ActivePortMapping, PortMappingAttemptError, PortMappingBackend,
    PortMappingLifecycle, PortMappingPlatform, PortMappingProtocol
};
// use crate::instance::mapped_listener_manager::;
use crate::common::{netns::NetNS, upnp};

struct RuntimePortMappingPlatform {
    net_ns: NetNS,
}

#[async_trait]
impl PortMappingPlatform for RuntimePortMappingPlatform {
    async fn establish_port_mapping(
        &self,
        backend: PortMappingBackend,
        protocal: PortMappingProtocol,
        local_listener: &url::Url,
    ) -> Result<Box<dyn ActivePortMapping>, PortMappingAttemptError> {
        upnp::establish_port_mapping(self.net_ns.clone(), backend, protocal, local_listener.clone()).await
    }

    fn spawn_port_mapping_lifecycle(
        &self,
        local_listener: url::Url,
        lifecycle: PortMappingLifecycle,
    ) {
        upnp::spawn_port_mapping_lifecycle(self.net_ns.clone(), local_listener, lifecycle);
    }
    async fn get_router_wan_ip(
        &self,
    ) -> Result<Ipv4Addr, anyhow::Error> {
        upnp::get_router_wan_ip(self.net_ns.clone()).await
    }
}

pub(crate) fn runtime_port_mapping_platform(net_ns: NetNS) -> Arc<dyn PortMappingPlatform> {
    Arc::new(RuntimePortMappingPlatform { net_ns })
}
