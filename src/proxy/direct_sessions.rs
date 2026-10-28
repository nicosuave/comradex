//! Reclaim warm direct sockets at capacity without interrupting accepted work.

use super::*;

#[derive(Default)]
pub(super) struct DirectSessions {
    next_id: AtomicU64,
    entries: StdMutex<HashMap<u64, Entry>>,
}

struct Entry {
    idle_since: Option<Instant>,
    evict: Option<oneshot::Sender<()>>,
}

pub(super) struct Session {
    registry: Arc<DirectSessions>,
    id: u64,
    evicted: oneshot::Receiver<()>,
}

impl DirectSessions {
    pub(super) fn register(self: &Arc<Self>) -> Session {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (evict, evicted) = oneshot::channel();
        self.entries.lock().expect("direct sessions").insert(
            id,
            Entry {
                idle_since: None,
                evict: Some(evict),
            },
        );
        Session {
            registry: self.clone(),
            id,
            evicted,
        }
    }

    pub(super) fn evict_oldest_idle(&self) -> bool {
        let mut entries = self.entries.lock().expect("direct sessions");
        let oldest = entries
            .iter()
            .filter(|(_, entry)| entry.evict.is_some())
            .filter_map(|(id, entry)| entry.idle_since.map(|since| (*id, since)))
            .min_by_key(|(_, since)| *since)
            .map(|(id, _)| id);
        let Some(id) = oldest else { return false };
        let evict = entries.get_mut(&id).unwrap().evict.take().unwrap();
        let _ = evict.send(());
        true
    }
}

impl Session {
    pub(super) fn set_idle(&self, idle: bool) {
        let mut entries = self.registry.entries.lock().expect("direct sessions");
        let entry = entries
            .get_mut(&self.id)
            .expect("registered direct session");
        if idle {
            entry.idle_since.get_or_insert_with(Instant::now);
        } else {
            entry.idle_since = None;
        }
    }

    pub(super) async fn evicted(&mut self) {
        let _ = (&mut self.evicted).await;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.registry
            .entries
            .lock()
            .expect("direct sessions")
            .remove(&self.id);
    }
}
