use async_std::sync::Arc;
use futures::StreamExt;
use std::time::Duration;
use zenoh::net::*;
use zenoh_dalgo::group::*;

#[async_std::main]
async fn main() {
    env_logger::init();
    let z = Arc::new(open(ConfigProperties::default()).await.unwrap());
    let mut zgc = ZGroupConfig::new(z.id().await, "demo-group".to_string());
    zgc.lease(Duration::from_secs(2));
    let n = 3; // @TODO: This should be a command line arg.
    let group = ZGroup::join(z.clone(), zgc).await;
    println!(">>> Waiting for a view of {} members to be established", n);
    if group.await_view_size(n, Duration::from_secs(10)).await {
        println!("Reached Group Size: {}", group.size().await);
    } else {
        println!(
            "Could not reach group size by allocated time, the group has only {} members",
            group.size().await
        );
    }
}
