use async_std::sync::{Arc, Condvar, Mutex};
use flume::unbounded;
use flume::{Receiver, Sender};
use futures::prelude::*;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use zenoh::net::protocol::core::PeerId;
use zenoh::net::queryable::EVAL;
use zenoh::net::{Query, QueryConsolidation, QueryTarget, Queryable, Sample, Session};

const ZGROUP_PREFIX: &str = "/zenoh/net/utils/group";
const MAX_START_LOOKOUT_DELAY: usize = 2;

#[derive(Serialize, Deserialize, Debug)]
struct ZGroupView {
    leader: String,
    gid: String,
    members: HashSet<String>,
}

#[derive(Serialize, Deserialize, Debug)]
enum ZGroupEvent {
    Join { mid: String },
    Leave { mid: String },
    NewLeader { mid: String },
    UpdatedGroupView { members: HashSet<String> },
}

pub struct ZGroup {
    gid: String,
    mid: String,
    leader: Arc<Mutex<String>>,
    members: Arc<Mutex<HashSet<String>>>,
    view_changed: Arc<Condvar>,
    lease: Duration,
    z: Arc<zenoh::net::Session>,
    group_events_rx: Arc<Mutex<Option<Receiver<ZGroupEvent>>>>,
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
                if ms.insert(mid.clone()) == true {
                    let mut u_evt = user_evt_tx.lock().await;
                    let mut l = leader.lock().await;
                    let mut new_leader = false;
                    if *l > mid {
                        *l = mid.clone();
                        new_leader = true;
                    }
                    match &*u_evt {
                        Some(tx) => {
                            tx.send(ZGroupEvent::Join { mid: mid.clone() });
                            if new_leader {
                                let _ = tx.send(ZGroupEvent::NewLeader { mid: mid.clone() });
                            }
                            view_changed.notify_all();
                        }
                        _ => {}
                    }
                }
            }
            ZGroupEvent::Leave { mid } => {
                log::debug!("ZGroupEvent::Leave ( mid: {} )", &mid);
                if mid == self_id {
                    return;
                } else {
                    if ms.remove(&mid) == true {
                        let u_evt = user_evt_tx.lock().await;
                        let mut l = leader.lock().await;
                        let mut new_leader = false;
                        if *l == mid {
                            *l = ms.iter().min().unwrap().into();
                            new_leader = true;
                        }
                        match &*u_evt {
                            Some(tx) => {
                                tx.send(ZGroupEvent::Leave { mid });
                                if new_leader {
                                    tx.send(ZGroupEvent::NewLeader { mid: l.clone() });
                                }
                            }
                            None => {}
                        }
                        view_changed.notify_all();
                    }
                }
            }
            ZGroupEvent::UpdatedGroupView { members } => {
                log::debug!(
                    "ZGroupEvent::UpdatedGroupView ( members:  {})",
                    members
                        .iter()
                        .fold(String::from(""), |a, b| format!("{} {}", a, b))
                );
                let left = ms.difference(&members);
                let joined = members.difference(&ms);
                let mut u_evt = user_evt_tx.lock().await;
                match &*u_evt {
                    Some(tx) => {
                        for l in left {
                            tx.send(ZGroupEvent::Leave {
                                mid: String::from(l),
                            });
                        }
                        for j in joined {
                            tx.send(ZGroupEvent::Join {
                                mid: String::from(j),
                            });
                        }
                    }
                    None => {}
                }
                *ms = members;
                let min: String = ms.iter().min().unwrap().into();
                let mut l = leader.lock().await;
                if *l != min {
                    *l = min.clone();
                }
                match &*u_evt {
                    Some(tx) => {
                        tx.send(ZGroupEvent::NewLeader { mid: min.into() });
                    }
                    _ => {}
                }
                view_changed.notify_all();
            }
            ZGroupEvent::NewLeader { mid } => {}
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
            });
        }
        query.reply(Sample {
            res_name: q_res.clone().into(),
            payload: self_id.clone().into_bytes().into(),
            data_info: None,
        })
    }
}

impl ZGroup {
    pub async fn join(gid: String, lease: Duration, z: Arc<zenoh::net::Session>) -> ZGroup {
        let mid = z.id().await;
        let leader = Arc::new(Mutex::new(mid.clone()));
        let members = Arc::new(Mutex::new(HashSet::new()));
        let qrbl_res = format!("/{}/{}/member/{}", ZGROUP_PREFIX, &gid, &mid);
        let query = format!("/{}/{}/member/*", ZGROUP_PREFIX, &gid);
        let evt_sub_res = format!("/{}/{}/event", ZGROUP_PREFIX, &gid);

        let view_changed = Arc::new(Condvar::new());

        let (evt_tx, evt_rx) = flume::unbounded::<ZGroupEvent>();
        let mid_evt = mid.clone();

        let ge_rx: Arc<Mutex<Option<Receiver<ZGroupEvent>>>> = Arc::new(Default::default());
        let ge_tx: Arc<Mutex<Option<Sender<ZGroupEvent>>>> = Arc::new(Default::default());

        let zg = ZGroup {
            gid,
            mid: mid.clone(),
            leader: leader.clone(),
            members: members.clone(),
            lease,
            view_changed: view_changed.clone(),
            z: z.clone(),
            group_events_rx: ge_rx,
            group_events_tx: ge_tx.clone(),
        };

        let f = local_event_loop(
            mid.clone(),
            members.clone(),
            leader.clone(),
            Arc::new(evt_rx),
            ge_tx.clone(),
            view_changed.clone(),
        );
        async_std::task::spawn(f);

        let zq = z.clone();
        log::debug!("Registering handler for Queriable");
        // Spawn task to handle queries
        async_std::task::spawn(query_handler(
            z.clone(),
            qrbl_res,
            mid.clone(),
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
        let mut initial_view = z
            .query(
                &query.into(),
                &mid,
                QueryTarget::default(),
                QueryConsolidation::default(),
            )
            .await
            .unwrap();

        let mut members = HashSet::new();
        while let Some(reply) = initial_view.next().await {
            let bs = reply.data.payload.get_vec();
            let omid = String::from_utf8(bs).unwrap();
            members.insert(omid);
        }
        evt_tx.send(ZGroupEvent::UpdatedGroupView { members });

        // Now we start the task tha refreshes the view if we are the member with the
        // smallest group-id
        zg
    }

    pub fn group_id(&self) -> &str {
        &self.gid
    }
    pub fn member_id(&self) -> &str {
        &self.mid
    }
    pub async fn leader_id(&self) -> String {
        self.leader.lock().await.clone()
    }
    pub async fn view(&self) -> HashSet<String> {
        let ms = self.members.lock().await;
        ms.clone()
    }

    pub async fn size(&self) -> usize {
        let ms = self.members.lock().await;
        ms.len()
    }

    pub async fn await_view_size(&self, n: usize, timeout: Duration) {
        if self.size().await < n {
            // @TODO: Add race with the timeout
            let mut ms = self.members.lock().await;
            while ms.len() < n {
                ms = self.view_changed.wait(ms).await;
            }
        }
    }
}

impl Drop for ZGroup {
    fn drop(&mut self) {
        // send leave message
    }
}
