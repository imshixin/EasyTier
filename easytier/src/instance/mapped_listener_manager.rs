use std::sync::Arc;

use async_trait::async_trait;
use crate::common::global_ctx::ArcGlobalCtx;
use easytier_core::listener::MappedListenerManager;
use crate::common::config::ConfigLoader;

pub struct CoreMappedListenerManager {
    global_ctx: ArcGlobalCtx
}

#[async_trait]
impl MappedListenerManager for CoreMappedListenerManager {
    async fn get_mapped_listeners(&self) -> Vec<url::Url> {
        self.global_ctx.config.get_mapped_listeners()
    }
    async fn add_mapped_listeners(&self, new_listener: &url::Url) {
        let mut config = self.global_ctx.config.get_mapped_listeners();
        for listener in &config {
            if listener == new_listener {
                return
            }
        }
        config.push(new_listener.clone());
        self.global_ctx.config.set_mapped_listeners(Some(config));
    }
    async fn remove_mapped_listeners(&self, old_listener: &url::Url) {
        let mut config = self.global_ctx.config.get_mapped_listeners();
        config.retain(|url| {
            if url != old_listener {
                tracing::info!("mytracing- manual remove mapped listener");
                false
            } else {
                true
            }
        });
        self.global_ctx.config.set_mapped_listeners(Some(config));
    }
}
pub(crate) fn runtime_mapped_listener_manager(global_ctx: ArcGlobalCtx) -> Arc<dyn MappedListenerManager> {
    Arc::new(CoreMappedListenerManager { global_ctx })
}
