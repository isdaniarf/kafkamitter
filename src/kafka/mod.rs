pub mod admin;
pub mod certs;
pub mod config;
pub mod consume;
pub mod group_offsets;
pub mod groups;
pub mod import;
pub mod metadata;
pub mod produce;
pub mod worker;

use std::collections::HashMap;
use std::rc::Rc;

use crate::model::profile::ConnectionProfile;
use worker::WorkerHandle;

#[derive(Default)]
pub struct KafkaService {
    workers: HashMap<String, Rc<WorkerHandle>>,
}

impl KafkaService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn worker(&mut self, profile: &ConnectionProfile, password: Option<&str>) -> Rc<WorkerHandle> {
        if let Some(existing) = self.workers.get(&profile.id) {
            return existing.clone();
        }
        let system_roots = if profile.needs_password() && profile.ca_location.is_none() {
            certs::system_ca_pem()
        } else {
            None
        };
        let base = config::base_config(profile, password, system_roots);
        let handle = Rc::new(WorkerHandle::spawn(&profile.name, base));
        self.workers.insert(profile.id.clone(), handle.clone());
        handle
    }

    pub fn existing(&self, profile_id: &str) -> Option<Rc<WorkerHandle>> {
        self.workers.get(profile_id).cloned()
    }

    pub fn disconnect(&mut self, profile_id: &str) {
        self.workers.remove(profile_id);
    }

    pub fn is_connected(&self, profile_id: &str) -> bool {
        self.workers.contains_key(profile_id)
    }
}
