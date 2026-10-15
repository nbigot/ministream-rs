//! In memory catalog of streams (shared by all the storage providers).

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::error::{Error, Result};
use crate::types::{SharedStreamInfo, StreamUuid};

#[derive(Default)]
pub struct Catalog {
    streams: RwLock<HashMap<StreamUuid, SharedStreamInfo>>,
}

impl Catalog {
    pub fn clear(&self) {
        self.streams.write().clear();
    }

    pub fn insert(&self, info: SharedStreamInfo) {
        let uuid = info.read().uuid;
        self.streams.write().insert(uuid, info);
    }

    pub fn remove(&self, stream_uuid: &StreamUuid) -> Option<SharedStreamInfo> {
        self.streams.write().remove(stream_uuid)
    }

    pub fn contains(&self, stream_uuid: &StreamUuid) -> bool {
        self.streams.read().contains_key(stream_uuid)
    }

    pub fn get(&self, stream_uuid: &StreamUuid) -> Result<SharedStreamInfo> {
        self.streams
            .read()
            .get(stream_uuid)
            .cloned()
            .ok_or_else(|| Error::not_found(format!("stream not found {stream_uuid}")))
    }

    /// Uuids of the streams, sorted by creation date.
    pub fn uuids(&self) -> Vec<StreamUuid> {
        let streams = self.streams.read();
        let mut list: Vec<_> = streams
            .values()
            .map(|info| {
                let info = info.read();
                (info.creation_date, info.uuid)
            })
            .collect();
        list.sort();
        list.into_iter().map(|(_, uuid)| uuid).collect()
    }

    pub fn len(&self) -> usize {
        self.streams.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
