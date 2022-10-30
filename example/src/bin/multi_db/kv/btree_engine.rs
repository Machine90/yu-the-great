use std::{
    collections::{BTreeMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
};

use application::{
    coprocessor::{
        listener::{proposal::RaftListener, Acl, RaftContext},
        ChangeReason,
    },
    RaftRole, SoftState,
};

use components::{
    bincode,
    common::protocol::{read_state::ReadState, GroupID},
    mailbox::multi::model::*,
    mailbox::{multi::{
        balance::{BalanceHelper, CheckPolicy, Config},
        model::{
            check::{CheckGroup, Perspective, Usage},
            *,
        },
    }, topo::PeerID},
    protos::raft_log_proto::Entry,
    torrent::partitions::key::Key,
    vendor::prelude::{lock::RwLock, DashMap},
};
use serde::{Deserialize, Serialize};
use std::ops::Bound;

use super::Operation;

pub struct BTreeEngine {
    db: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    conf: Config,
}

impl BTreeEngine {
    pub fn new() -> Self {
        Self {
            db: RwLock::new(BTreeMap::new()),
            conf: Config {
                check_interval_millis: 15000,
                check_policy: CheckPolicy::Fixed {
                    min_group_size: 64,
                    max_group_size: 5120,
                },
            },
        }
    }

    pub fn put(&self, group: GroupID, key: Vec<u8>, val: Vec<u8>) -> i64 {
        let k_size = key.len() as i64;
        let v_size = val.len() as i64;
        if let Some(old_val) = self.db.write().insert(key, val) {
            let ov_size = old_val.len() as i64;
            v_size - ov_size
        } else {
            k_size + v_size
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.read().get(key).map(|v| v.clone())
    }

    pub fn delete(&self, group: GroupID, key: &[u8]) {
        if let Some(v) = self.db.write().remove(key) {
            let size = key.len() + v.len();
        }
    }
}

impl Acl for BTreeEngine {}

impl BalanceHelper for BTreeEngine {
    #[inline]
    fn conf(&self) -> Config {
        self.conf
    }

    fn groups_usage(&self, targets: &mut Vec<CheckGroup>) {
        for mut target in targets.iter_mut() {
            let s = target.from.left_bound();
            let e = target.to.right_bound();

            let used: u64 = self
                .db
                .read()
                .range((s, e))
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum();

            let size = 6048000;
            let avail = if size >= used { size - used } else { 0 };
            let usg = Usage::new(used, Perspective::Node { size });
            // println!("GU: {:?} {:?}", target.id, usg);
            target.gu = Some(usg);
        }
    }

    fn split_keys(&self, should_splits: &mut Vec<CheckGroup>) {
        for target in should_splits {
            let CheckGroup { from, to, .. } = target;

            let from_key = from.left_bound();
            let to_key = to.right_bound();

            let mut incremental = 0;
            let split_size = target.get_split_size();
            if split_size.is_none() {
                continue;
            }
            let split_size = split_size.unwrap() as usize;

            for (k, v) in self.db.read().range((from_key, to_key)) {
                incremental += k.len() + v.len();

                if split_size <= incremental {
                    println!("split key: {:?}", String::from_utf8(v.clone()));
                    target.set_split_key(k);
                    break;
                }
            }
        }
    }

    fn clear_partitions(&self, partition: VecDeque<(GroupID, Key, Key)>) -> std::io::Result<()> {
        for (peer, from, to) in partition {
            println!(
                "[Balancer] clear peer: {:?}, from: {:?} to: {:?}",
                peer, from, to
            );
        }
        Ok(())
    }
}

#[application::async_trait]
impl RaftListener for BTreeEngine {
    async fn handle_write(&self, ctx: &RaftContext, entries: &[Entry]) -> std::io::Result<i64> {
        let group = ctx.group_id;
        let mut written = 0;
        for ent in entries {
            let op = bincode::deserialize(&ent.data[..]);
            if let Err(e) = op {
                println!("decode err: {:?}", e);
                continue;
            }
            let op: Operation = op.unwrap();
            match op {
                Operation::Put(key, val) => {
                    written += self.put(group, key, val);
                }
                _ => (),
            }
        }
        Ok(written)
    }

    async fn handle_read(&self, _: &RaftContext, read_states: &mut ReadState) {
        let op = bincode::deserialize::<Operation>(&read_states.request_ctx);
        if let Err(e) = op {
            return;
        }
        match op.unwrap() {
            Operation::Get(key) => {
                read_states.request_ctx = self
                    .get(key.as_slice())
                    .map(|v| v.clone())
                    .unwrap_or_default()
            }
            _ => (),
        }
    }

    fn on_soft_state_change(
        &self,
        peer_id: PeerID,
        _: SoftState,
        current_state: SoftState,
        reason: ChangeReason,
    ) {
        let (group, node) = peer_id;
        let cur_role = current_state.raft_state;
        if cur_role == RaftRole::Leader {
            println!(
                "[Group-{:?}] node {:?} became leader, because: {:?}",
                group,
                node,
                reason.describe()
            );
        }
    }
}
