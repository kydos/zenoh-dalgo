use async_std::sync::Arc;
use std::time::Duration;
use zenoh::net::*;
use zenoh_dalgo::group::ZGroup;

#[async_std::main]
async fn main() {
    env_logger::init();
    let z = Arc::new(open(ConfigProperties::default()).await.unwrap());
    let group = ZGroup::join("demo-group".to_string(), Duration::from_secs(10), z).await;
    async_std::task::sleep(Duration::from_secs(60)).await;
}
