use async_std::sync::Arc;
use std::time::Duration;
use zenoh::net::*;
use zenoh_dalgo::group::*;

#[async_std::main]
async fn main() {
    env_logger::init();
    let z = Arc::new(open(ConfigProperties::default()).await.unwrap());
    let mut zgc = ZGroupConfig::new(z.clone(), "demo-group".to_string());
    zgc.lease(Duration::from_secs(5));

    let group = ZGroup::join(zgc).await;
    async_std::task::sleep(Duration::from_secs(600)).await;
}
