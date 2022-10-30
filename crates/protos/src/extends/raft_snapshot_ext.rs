use std::io::{Error, ErrorKind};

use crate::{
    bincode,
    raft_log_proto::{ConfState, Snapshot, SnapshotMetadata},
    raft_payload_proto::Message as RaftMessage,
};

#[allow(unused)]
use prost::{bytes::BytesMut, Message};
use serde::{de, ser, Deserialize, Serialize};
use vendor::prelude::*;

static SNAP_META: Singleton<SnapshotMetadata> = Singleton::INIT;
impl Snapshot {
    pub fn get_metadata(&self) -> &SnapshotMetadata {
        match self.metadata.as_ref() {
            Some(meta) => meta,
            _ => SNAP_META.get(|| SnapshotMetadata::default()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.get_metadata().index == 0
    }

    pub fn get_metadata_mut(&mut self) -> &mut SnapshotMetadata {
        if self.metadata.is_none() {
            self.metadata = Some(SnapshotMetadata::default())
        }
        self.metadata.as_mut().unwrap()
    }

    // Take field
    pub fn take_metadata(&mut self) -> SnapshotMetadata {
        self.metadata
            .take()
            .unwrap_or_else(|| SnapshotMetadata::default())
    }

    #[allow(unused)]
    pub fn try_get_data<'a, T: Deserialize<'a>>(&'a self) -> std::io::Result<T> {
        let data = bincode::deserialize::<T>(&self.data);
        if let Err(e) = data {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("decode snapshot data error: {:?}", e),
            ));
        }
        Ok(data.unwrap())
    }

    #[allow(unused)]
    pub fn set_data<T: Serialize>(&mut self, value: &T) -> std::io::Result<()> {
        let data = bincode::serialize(value);
        if let Err(e) = data {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                format!("encode snapshot data error: {:?}", e),
            ));
        }
        self.data = data.unwrap();
        Ok(())
    }
}

static CONF_STATE: Singleton<ConfState> = Singleton::INIT;
impl SnapshotMetadata {
    pub fn take_conf_state(&mut self) -> ConfState {
        self.conf_state.take().unwrap_or_else(|| Default::default())
    }

    pub fn get_conf_state(&self) -> &ConfState {
        let def_conf_state = CONF_STATE.get(|| ConfState::default());
        self.conf_state.as_ref().unwrap_or(def_conf_state)
    }
}

static SNAP: Singleton<Snapshot> = Singleton::INIT;
impl RaftMessage {
    pub fn get_snapshot(&self) -> &Snapshot {
        let def_snap = SNAP.get(|| Snapshot::default());
        self.snapshot.as_ref().unwrap_or(&def_snap)
    }

    pub fn get_snapshot_mut(&mut self) -> &mut Snapshot {
        if self.snapshot.is_none() {
            self.snapshot = Some(Snapshot::default());
        }
        self.snapshot.as_mut().unwrap()
    }

    pub fn take_snapshot(&mut self) -> Snapshot {
        self.snapshot.take().unwrap_or_else(|| Snapshot::default())
    }
}

impl serde::Serialize for Snapshot {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut buf = BytesMut::new();
        let e = self.encode(&mut buf);
        if let Err(e) = e {
            return Err(ser::Error::custom(e));
        }
        let ser = serializer.serialize_bytes(&buf[..])?;
        Ok(ser)
    }
}

impl<'de> Deserialize<'de> for Snapshot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let buf: Vec<u8> = Deserialize::deserialize(deserializer)?;
        let d = Snapshot::decode(&buf[..]);
        if let Err(e) = d {
            return Err(de::Error::custom(e));
        }
        Ok(d.unwrap())
    }
}
