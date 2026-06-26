//! Vector clocks — the causal-ordering primitive for conflict resolution.
//!
//! Each device keeps a monotonically increasing counter. Comparing two clocks tells us
//! whether one change causally precedes another or whether they are *concurrent* (a real
//! conflict that later phases resolve via conflicted-copies). Wall-clock time is never used,
//! so device clock skew can't cause silent data loss.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Causal relationship between two vector clocks.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Causality {
    /// `self` happened strictly before `other`.
    Before,
    /// `self` happened strictly after `other`.
    After,
    /// Identical clocks.
    Equal,
    /// Neither dominates — a genuine conflict.
    Concurrent,
}

/// A per-device logical clock: `device id -> counter`.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VClock(BTreeMap<String, u64>);

impl VClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bump this device's counter. Call on every local mutation.
    pub fn increment(&mut self, device: &str) {
        *self.0.entry(device.to_string()).or_insert(0) += 1;
    }

    /// This device's current counter (0 if never seen).
    pub fn get(&self, device: &str) -> u64 {
        self.0.get(device).copied().unwrap_or(0)
    }

    /// Merge in another clock, taking the per-device maximum.
    pub fn merge(&mut self, other: &VClock) {
        for (device, &counter) in &other.0 {
            let entry = self.0.entry(device.clone()).or_insert(0);
            if counter > *entry {
                *entry = counter;
            }
        }
    }

    /// Compare causal order against another clock.
    pub fn compare(&self, other: &VClock) -> Causality {
        let mut self_ahead = false;
        let mut other_ahead = false;
        for device in self.0.keys().chain(other.0.keys()) {
            let a = self.get(device);
            let b = other.get(device);
            if a > b {
                self_ahead = true;
            }
            if b > a {
                other_ahead = true;
            }
        }
        match (self_ahead, other_ahead) {
            (false, false) => Causality::Equal,
            (true, false) => Causality::After,
            (false, true) => Causality::Before,
            (true, true) => Causality::Concurrent,
        }
    }
}
