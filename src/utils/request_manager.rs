use std::collections::HashMap;

use uuid::Uuid;

/// Manages a fixed set of request identifiers identified by logical slots.
///
/// Each slot holds an optional `Uuid` representing the latest outbound request.
/// Consumers can update, clear, or lazily initialise ids without sprinkling
/// `Option<Uuid>` state across the strategy.
#[derive(Debug, Default, Clone)]
pub struct RequestIdManager {
    slots: HashMap<&'static str, Option<Uuid>>,
}

impl RequestIdManager {
    /// Create a new manager initialised with the provided slot names.
    ///
    /// Example:
    /// ```
    /// let manager = RequestIdManager::with_slots(["jito_fee", "user_wallets"]);
    /// ```
    pub fn with_slots<const N: usize>(slots: [&'static str; N]) -> Self {
        let mut map = HashMap::with_capacity(N);
        for key in slots {
            map.insert(key, None);
        }
        Self { slots: map }
    }

    /// Returns the current request id stored under `slot`.
    pub fn get(&self, slot: &'static str) -> Option<Uuid> {
        self.slots.get(slot).copied().flatten()
    }

    /// Insert or replace a request id for the given slot, returning the previous value.
    ///
    /// If the slot does not exist it will be created.
    pub fn set(&mut self, slot: &'static str, request_id: Uuid) -> Option<Uuid> {
        self.slots.entry(slot).or_insert(None).replace(request_id)
    }

    /// Insert or replace a request id for the given slot, returning the new value.
    pub fn set_default(&mut self, slot: &'static str) -> Uuid {
        let request_id = Uuid::new_v4();
        self.slots
            .entry(slot)
            .or_insert(None)
            .replace(request_id.clone());
        request_id
    }

    /// Replace the request id only when it differs, returning `bool` to signal change.
    pub fn switch(&mut self, slot: &'static str, request_id: Uuid) -> bool {
        match self.slots.entry(slot).or_insert(None) {
            Some(existing) if existing == &request_id => false,
            entry => {
                *entry = Some(request_id);
                true
            }
        }
    }

    /// Ensure a request id exists for the slot by generating one if absent.
    pub fn ensure(&mut self, slot: &'static str) -> Uuid {
        *self
            .slots
            .entry(slot)
            .or_insert(None)
            .get_or_insert_with(Uuid::new_v4)
    }

    /// Clear the stored request id and return it.
    pub fn clear(&mut self, slot: &'static str) -> Option<Uuid> {
        self.slots.get_mut(slot).and_then(|value| value.take())
    }

    /// Clear every slot and return the previous state map.
    pub fn clear_all(&mut self) -> HashMap<&'static str, Option<Uuid>> {
        let mut previous = HashMap::with_capacity(self.slots.len());
        for (slot, value) in self.slots.iter_mut() {
            previous.insert(*slot, value.take());
        }
        previous
    }

    /// Access the internal optional value mutably for custom updates.
    pub fn slot_mut(&mut self, slot: &'static str) -> &mut Option<Uuid> {
        self.slots.entry(slot).or_insert(None)
    }

    /// Iterate over all slot/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, Option<Uuid>)> + '_ {
        self.slots.iter().map(|(slot, value)| (*slot, *value))
    }
}
