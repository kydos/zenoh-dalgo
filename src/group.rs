use async_std::sync::{Arc, Condvar, Mutex};
use flume::{Receiver, Sender};
use futures::prelude::*;
use futures::select;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use zenoh::net::queryable::EVAL;
use zenoh::net::{ConsolidationMode, QueryConsolidation, QueryTarget, Sample, Session, SubInfo};

const ZGROUP_PREFIX: &str = "/zenoh/net/utils/group";
const MAX_START_LOOKOUT_DELAY: usize = 2;
const VIEW_REFRESH_LEASE_RATIO: f32 = 0.75f32;
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(1);
const DEFAULT_LEASE: Duration = Duration::from_secs(18);

#[derive(Serialize, Deserialize, Debug)]
pub enum ZGroupEvent {
    Join {
        mid: String,
    },
    Leave {
        mid: String,
    },
    NewLeader {
        mid: String,
    },
    UpdatedGroupView {
        source: String,
        members: HashSet<String>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Member {
    mid: String,
    info: Option<String>,
    lease: Duration,
}

impl Member {
    pub fn new(mid: &str) -> Member {
        Member {
            mid: String::from(mid),
            info: None,
            lease: DEFAULT_LEASE,
        }
    }
    pub fn info(&mut self, i: &str) -> &mut Self {
        self.info = Some(String::from(i));
        self
    }
    pub fn lease(&mut self, d: Duration) -> &mut Self {
        self.lease = d;
        self
    }
}

pub struct ZGroupConfig {
    gid: String,
    mid: String,
    lease: Duration,
    view_refresh_lease_ratio: f32,
    query_timeout: Duration,
}

impl ZGroupConfig {
    pub fn new(member_id: String, group_id: String) -> ZGroupConfig {
        let mid = member_id;
        ZGroupConfig {
            gid: group_id,
            mid,
            lease: DEFAULT_LEASE,
            view_refresh_lease_ratio: VIEW_REFRESH_LEASE_RATIO,
            query_timeout: DEFAULT_QUERY_TIMEOUT,
        }
    }
    pub fn member_id(&mut self, id: String) -> &mut Self {
        self.mid = id;
        self
    }

    pub fn lease(&mut self, d: Duration) -> &mut Self {
        self.lease = d;
        self
    }

    pub fn view_refresh_lease_ratio(&mut self, r: f32) -> &mut Self {
        self.view_refresh_lease_ratio = r;
        self
    }
    pub fn query_timeout(&mut self, d: Duration) -> &mut Self {
        self.query_timeout = d;
        self
    }
}
pub struct ZGroup {
    gid: String,
    mid: String,
    leader: Arc<Mutex<String>>,
    members: Arc<Mutex<HashSet<String>>>,
    view_changed: Arc<Condvar>,
    lease: Duration,
    group_events_tx: Arc<Mutex<Option<Sender<ZGroupEvent>>>>,
}

async fn local_event_loop(
    self_id: String,
    members: Arc<Mutex<HashSet<String>>>,
    leader: Arc<Mutex<String>>,
    evt_rx: Arc<Receiver<ZGroupEvent>>,
    user_evt_tx: Arc<Mutex<Option<Sender<ZGroupEvent>>>>,
    view_changed: Arc<Condvar>,
) {
    while let Some(evt) = evt_rx.stream().next().await {
        let mut ms = members.lock().await;
        match evt {
            ZGroupEvent::Join { mid } => {
                log::debug!("ZGroupEvent::Join ( mid: {} )", &mid);
                if ms.insert(mid.clone()) {
                    let mut lg = leader.lock().await;
                    let mut new_leader = false;
                    if *lg > mid {
                        *lg = mid.clone();
                        new_leader = true;
                    }
                    drop(lg);
                    let mut u_evt = user_evt_tx.lock().await;
                    if let Some(tx) = &*u_evt {
                        if tx.send(ZGroupEvent::Join { mid: mid.clone() }).is_err() {
                            *u_evt = None;
                        } else if new_leader {
                            // Worse case we'll clean-up on next event
                            let _ = tx.send(ZGroupEvent::NewLeader { mid: mid.clone() });
                        }
                        view_changed.notify_all();
                    }
                }
            }
            ZGroupEvent::Leave { mid } => {
                log::debug!("ZGroupEvent::Leave ( mid: {} )", &mid);
                if mid == self_id {
                    return;
                } else if ms.remove(&mid) {
                    let mut lg = leader.lock().await;
                    let mut new_leader = false;
                    if *lg == mid {
                        *lg = ms.iter().min().unwrap().into();
                        new_leader = true;
                    }
                    let l = (*lg).clone();
                    drop(lg);
                    let mut u_evt = user_evt_tx.lock().await;
                    match &*u_evt {
                        Some(tx) => {
                            if tx.send(ZGroupEvent::Leave { mid }).is_err() {
                                *u_evt = None;
                            } else if new_leader {
                                // Worse case we'll clean-up on next event
                                let _ = tx.send(ZGroupEvent::NewLeader { mid: l });
                            }
                        }
                        None => {}
                    }
                    view_changed.notify_all();
                }
            }
            ZGroupEvent::UpdatedGroupView { source, members } => {
                log::debug!(
                    "ZGroupEvent::UpdatedGroupView ( source: {}\n, members:  {})",
                    source,
                    members
                        .iter()
                        .fold(String::from("\n"), |a, b| format!("\t{} \n\t{}", a, b))
                );
                let left = ms.difference(&members);
                let joined = members.difference(&ms);
                let u_evt = user_evt_tx.lock().await;
                match &*u_evt {
                    Some(tx) => {
                        for l in left {
                            if tx
                                .send(ZGroupEvent::Leave {
                                    mid: String::from(l),
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                        for j in joined {
                            if tx
                                .send(ZGroupEvent::Join {
                                    mid: String::from(j),
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    }
                    None => {}
                }
                *ms = members;
                let min: String = ms.iter().min().unwrap().into();
                let mut l = leader.lock().await;
                if *l != min {
                    *l = min.clone();
                    if let Some(tx) = &*u_evt {
                        let _ = tx.send(ZGroupEvent::NewLeader { mid: min });
                    }
                }
                view_changed.notify_all();
            }
            ZGroupEvent::NewLeader { mid } => {
                let mut u_evt = user_evt_tx.lock().await;
                match &*u_evt {
                    Some(tx) => {
                        if tx.send(ZGroupEvent::NewLeader { mid }).is_err() {
                            *u_evt = None;
                        }
                    }
                    None => (),
                }
            }
        }
    }
}

async fn query_handler(
    z: Arc<Session>,
    q_res: String,
    self_id: String,
    tx: Arc<Sender<ZGroupEvent>>,
) {
    let mut queryable = z
        .declare_queryable(&q_res.clone().into(), EVAL)
        .await
        .unwrap();
    while let Some(query) = queryable.receiver().next().await {
        log::debug!("Handling Query");
        if query.predicate != self_id {
            tx.send(ZGroupEvent::Join {
                mid: query.predicate.clone(),
            })
            .unwrap();
        }
        query.reply(Sample {
            res_name: q_res.clone(),
            payload: self_id.clone().into_bytes().into(),
            data_info: None,
        })
    }
}
async fn zenoh_event_handler(
    z: Arc<Session>,
    evt_res_id: u64,
    lease: Duration,
    leader: Arc<Mutex<String>>,
    tx: Arc<Sender<ZGroupEvent>>,
) {
    let mut sub = z
        .declare_subscriber(&evt_res_id.into(), &SubInfo::default())
        .await
        .unwrap();
    loop {
        select!(
            evt = sub.receiver().next().fuse() =>  {
               let sample = evt.unwrap();
                match bincode::deserialize::<ZGroupEvent>(&sample.payload.to_vec()) {
                    Ok(ge) => {
                        log::debug!("Received zenoh group event");
                        tx.send(ge).unwrap();
                    },
                    Err(e) => {
                        log::error!("Failed to unmarshal data: {}", e);
                    }
                }

            },
            _ = async_std::task::sleep(lease).fuse() => {
                log::debug!("Timed-out on view update zenoh event, declaring leader failed");
                let l = String::from(&*leader.lock().await);
                tx.send(ZGroupEvent::Leave { mid: l }).unwrap();
            }
        )
    }
}
async fn refresh_view(
    z: Arc<Session>,
    query: String,
    self_id: String,
    evt_res_id: u64,
    lease: Duration,
    query_timeout: Duration,
    tx: Arc<Sender<ZGroupEvent>>,
    leader: Arc<Mutex<String>>,
) {
    // @TODO: Deal with termination
    loop {
        let sleep_time = lease.mul_f32(VIEW_REFRESH_LEASE_RATIO);
        log::debug!("Sleeping for: {:?} secs", sleep_time);
        async_std::task::sleep(sleep_time).await;
        let qc = QueryConsolidation {
            first_routers: ConsolidationMode::None,
            last_router: ConsolidationMode::None,
            reception: ConsolidationMode::None,
        };
        let l = String::from(&*leader.lock().await);

        if l == self_id {
            log::debug!(
                "This member ({}) is not the leader, going back to sleep",
                &self_id
            );
            let reply = z
                .query(&query.clone().into(), &self_id, QueryTarget::default(), qc)
                .await;
            let mut members = HashSet::new();
            let f = async {
                if let Ok(mut new_view) = reply {
                    while let Some(m) = new_view.next().await {
                        members.insert(String::from_utf8(m.data.payload.get_vec()).unwrap());
                    }
                }
            };
            select! {
                _ = f.fuse() => (),
                _ = async_std::task::sleep(query_timeout).fuse() => ()
            };

            let uge = ZGroupEvent::UpdatedGroupView {
                source: self_id.clone(),
                members,
            };
            let buf = bincode::serialize(&uge).unwrap();
            tx.send(uge).unwrap();
            z.write(&evt_res_id.into(), buf.into());
        }
    }
}
impl ZGroup {
    /// Joins a new group using the provided configuration.
    pub async fn join(z: Arc<Session>, config: ZGroupConfig) -> ZGroup {
        let leader = Arc::new(Mutex::new(config.mid.clone()));
        let members = Arc::new(Mutex::new(HashSet::new()));
        let qrbl_res = format!("/{}/{}/member/{}", ZGROUP_PREFIX, &config.gid, &config.mid);
        let query = format!("/{}/{}/member/*", ZGROUP_PREFIX, &config.gid);
        let evt_res = format!("/{}/{}/event", ZGROUP_PREFIX, &config.gid);
        let evt_res_id = z.declare_resource(&evt_res.clone().into()).await.unwrap();
        let view_changed = Arc::new(Condvar::new());

        let (evt_tx, evt_rx) = flume::unbounded::<ZGroupEvent>();
        let ge_tx: Arc<Mutex<Option<Sender<ZGroupEvent>>>> = Arc::new(Default::default());

        let zg = ZGroup {
            gid: config.gid.clone(),
            mid: config.mid.clone(),
            leader: leader.clone(),
            members: members.clone(),
            lease: config.lease,
            view_changed: view_changed.clone(),
            group_events_tx: ge_tx.clone(),
        };

        let f = local_event_loop(
            config.mid.clone(),
            members.clone(),
            leader.clone(),
            Arc::new(evt_rx),
            ge_tx.clone(),
            view_changed.clone(),
        );
        async_std::task::spawn(f);

        log::debug!("Registering handler for Queriable");
        // Spawn task to handle queries
        async_std::task::spawn(query_handler(
            z.clone(),
            qrbl_res,
            config.mid.clone(),
            Arc::new(evt_tx.clone()),
        ));

        // Now we are already answering queries, but it is good to add some non determinism
        // on the timing with which we issue a query to figure out who is around to create
        // our initial view
        let ratio = (rand::random::<usize>() as f32) / (usize::MAX as f32);
        log::debug!("Sleep Ratio: {}", ratio);
        async_std::task::sleep(Duration::from_secs_f32(
            (MAX_START_LOOKOUT_DELAY as f32) * ratio,
        ))
        .await;
        log::debug!("Issuing Query: {}", &query);
        let qc = QueryConsolidation {
            first_routers: ConsolidationMode::None,
            last_router: ConsolidationMode::None,
            reception: ConsolidationMode::None,
        };
        let mut initial_view = z
            .query(
                &query.clone().into(),
                &config.mid,
                QueryTarget::default(),
                qc,
            )
            .await
            .unwrap();

        let mut members = HashSet::new();
        while let Some(reply) = initial_view.next().await {
            let bs = reply.data.payload.get_vec();
            let omid = String::from_utf8(bs).unwrap();
            members.insert(omid);
        }
        evt_tx
            .send(ZGroupEvent::UpdatedGroupView {
                source: config.mid.clone(),
                members,
            })
            .unwrap();

        // Now we start the task tha refreshes the view if we are the member with the
        // smallest group-id
        let rv = refresh_view(
            z.clone(),
            query.clone(),
            config.mid.clone(),
            evt_res_id,
            config.lease,
            config.query_timeout,
            Arc::new(evt_tx.clone()),
            leader.clone(),
        );
        async_std::task::spawn(rv);
        let zeh = zenoh_event_handler(
            z.clone(),
            evt_res_id,
            config.lease,
            leader.clone(),
            Arc::new(evt_tx.clone()),
        );
        async_std::task::spawn(zeh);
        zg
    }

    /// Returns the group identifier.
    pub fn group_id(&self) -> &str {
        &self.gid
    }

    /// Returns this member identifier.
    pub fn member_id(&self) -> &str {
        &self.mid
    }
    /// Returns the current group leader.
    pub async fn leader_id(&self) -> String {
        self.leader.lock().await.clone()
    }
    /// Returns the current group view, in other terms the list
    /// of group members.
    pub async fn view(&self) -> HashSet<String> {
        let ms = self.members.lock().await;
        ms.clone()
    }
    /// Returns the current group size.
    pub async fn size(&self) -> usize {
        let ms = self.members.lock().await;
        ms.len()
    }
    /// Waits for a view size to be established within a given time and if
    /// the view is established it returns *true* and *false* otherwise.
    pub async fn await_view_size(&self, n: usize, timeout: Duration) -> bool {
        if self.size().await < n {
            let f1 = async {
                let mut ms = self.members.lock().await;
                while ms.len() < n {
                    ms = self.view_changed.wait(ms).await;
                }
                true
            };
            let res: bool = select! {
                _ = f1.fuse() => true,
                _ = async_std::task::sleep(timeout).fuse() => false
            };
            res
        } else {
            true
        }
    }
    // Returns the group lease duration.
    pub fn lease(&self) -> Duration {
        self.lease
    }
    /// Returns a receivers that will allow to receive notifications for group events.
    /// Notice that there can be a single subscription at the time, each call to subscribe
    /// will cancel the previous subscription.
    pub async fn subscribe(&self) -> Receiver<ZGroupEvent> {
        let (tx, rx) = flume::unbounded();
        *self.group_events_tx.lock().await = Some(tx);
        rx
    }
}

impl Drop for ZGroup {
    fn drop(&mut self) {
        // send leave message
    }
}
