// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::types::{Flag, Incarnation, MVGroupError, ShiftedTxnIndex, TxnIndex, Version};
use aptos_types::write_set::TransactionWrite;
use claims::assert_some;
use crossbeam::utils::CachePadded;
use dashmap::DashMap;
use serde::Serialize;
use std::{
    collections::{btree_map::BTreeMap, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    sync::Arc,
};

struct GroupEntry<V> {
    // Note: can be a raw pointer (different data-structure holds the value during the
    // lifetime), but would require unsafe access.
    value: Arc<V>,
    incarnation: Incarnation,
    flag: Flag,
}

impl<V: TransactionWrite> GroupEntry<V> {
    fn new(value: Arc<V>, incarnation: Incarnation) -> Self {
        Self {
            value,
            incarnation,
            flag: Flag::Done,
        }
    }
}

/// Represents a group value, i.e. a key that does not correspond to a single value,
/// but instead a collection of values each associated with a tag.
///
/// Implementation note: due to DashMap in VersionedGroupData, the updates are atomic.
/// If this changes, we must maintain invariants on insertion / deletion order among
/// members (e.g. versioned_map then idx_to_update then member_tags, deletion opposite).
pub(crate) struct VersionedGroupValue<T, V> {
    /// While versioned_map maps tags to versioned entries for the tag, idx_to_update
    /// maps a transaction index to all corresponding group updates. ShiftedTxnIndex is used
    /// to dedicated index 0 for base (storage version, prior to block execution) values.
    versioned_map: HashMap<T, BTreeMap<ShiftedTxnIndex, CachePadded<GroupEntry<V>>>>,
    /// Note: if we do not garbage collect final idx_to_update contents until the end of
    /// block execution (lifetime of the data-structure), then we can have other structures
    /// hold raw pointers to the values as an optimization.
    idx_to_update: BTreeMap<ShiftedTxnIndex, CachePadded<HashMap<T, Arc<V>>>>,

    /// Group membership is addressed in the AIP:
    /// https://github.com/aptos-foundation/AIPs/blob/main/aips/aip-9.md
    ///
    /// In summary, existing resources may not be added to a group, and existing members
    /// may not be removed from a group. However, a newly created resource may become
    /// a member, and also only members whose corresponding instances exist will be stored
    /// in a given group at a given time.
    ///
    /// We maintain the list of all tags the key has been associated with (either
    /// in storage, or in any of the writes), and also use it to determine the 'group_size'
    /// when requested. Group size is computed as the sum of sizes (length of raw bytes)
    /// of all latest inner values of all member tags, plus serialized length of tags.
    member_tags: HashSet<T>,

    /// Group contents corresponding to the latest committed version.
    committed_group: HashMap<T, Arc<V>>,
}

/// Maps each key (access path) to an internal VersionedValue.
pub struct VersionedGroupData<K, T, V> {
    group_values: DashMap<K, VersionedGroupValue<T, V>>,
}

impl<T: Hash + Clone + Debug + Eq + Serialize, V: TransactionWrite> Default
    for VersionedGroupValue<T, V>
{
    fn default() -> Self {
        Self {
            versioned_map: HashMap::new(),
            idx_to_update: BTreeMap::new(),
            member_tags: HashSet::new(),
            committed_group: HashMap::new(),
        }
    }
}

impl<T: Hash + Clone + Debug + Eq + Serialize, V: TransactionWrite> VersionedGroupValue<T, V> {
    fn write(
        &mut self,
        shifted_idx: ShiftedTxnIndex,
        incarnation: Incarnation,
        values: impl IntoIterator<Item = (T, V)>,
    ) {
        let arc_map = values
            .into_iter()
            .map(|(tag, v)| {
                let arc_v = Arc::new(v);

                // Update versioned_map.
                let tag_entry = self.versioned_map.entry(tag.clone()).or_default();
                tag_entry.insert(
                    shifted_idx.clone(),
                    CachePadded::new(GroupEntry::new(arc_v.clone(), incarnation)),
                );
                // Update member tags.
                self.member_tags.insert(tag.clone());

                (tag, arc_v)
            })
            .collect();

        let zero = ShiftedTxnIndex::zero();
        let base_idx = shifted_idx == zero;

        self.idx_to_update
            .insert(shifted_idx, CachePadded::new(arc_map));
        if base_idx {
            self.commit_idx(zero);
        }
    }

    fn mark_estimate(&mut self, txn_idx: TxnIndex) {
        let shifted_idx = ShiftedTxnIndex::new(txn_idx);
        let idx_updates = self
            .idx_to_update
            .get(&shifted_idx)
            .expect("Group updates must exist at the index to mark estimate");

        // estimate flag lives in GroupEntry, w. value in versioned_map to simplify reading
        // based on txn_idx and tag. marking estimates occurs per txn (data MVHashMap exposes
        // the interface for txn_idx & key). Hence, we must mark tags individually.
        for (tag, _) in idx_updates.iter() {
            self.versioned_map
                .get_mut(tag)
                .expect("Versioned entry must exist for tag")
                .get_mut(&shifted_idx)
                .expect("Versioned entry must exist")
                .flag = Flag::Estimate;
        }
    }

    fn delete(&mut self, txn_idx: TxnIndex) {
        let shifted_idx = ShiftedTxnIndex::new(txn_idx);
        // Delete idx updates first, then entries.
        let idx_updates = self
            .idx_to_update
            .remove(&shifted_idx)
            .expect("Group updates must exist at the index to mark estimate");

        // Similar to mark_estimate, need to delete an individual entry for each tag.
        for (tag, _) in idx_updates.iter() {
            assert_some!(
                self.versioned_map
                    .get_mut(tag)
                    .expect("Versioned entry must exist for tag")
                    .remove(&shifted_idx),
                "Entry for tag / idx must exist to be deleted"
            );
        }
    }

    // Records and returns pointers for the latest committed value for each tag in the group.
    fn commit_idx(&mut self, shifted_idx: ShiftedTxnIndex) -> HashMap<T, Arc<V>> {
        let idx_updates = self
            .idx_to_update
            .get(&shifted_idx)
            .expect("Group updates must exist at the index to commit");
        for (tag, v) in idx_updates.iter() {
            if v.is_deletion() {
                self.committed_group.remove(tag);
            } else {
                self.committed_group.insert(tag.clone(), v.clone());
            }
        }

        self.committed_group.clone()
    }

    fn get_latest_tagged_value(
        &self,
        txn_idx: TxnIndex,
        tag: &T,
    ) -> Result<(Arc<V>, Version), MVGroupError> {
        self.versioned_map
            .get(tag)
            .ok_or(MVGroupError::Uninitialized)
            .and_then(|tree| {
                match tree
                    .range(ShiftedTxnIndex::zero()..ShiftedTxnIndex::new(txn_idx))
                    .next_back()
                {
                    Some((idx, entry)) => {
                        if entry.flag == Flag::Estimate {
                            Err(MVGroupError::Dependency(
                                idx.idx().expect("May not depend on storage version"),
                            ))
                        } else {
                            Ok((
                                entry.value.clone(),
                                idx.idx().map(|idx| (idx, entry.incarnation)),
                            ))
                        }
                    },
                    None => {
                        if self.idx_to_update.contains_key(&ShiftedTxnIndex::zero()) {
                            Err(MVGroupError::TagNotFound)
                        } else {
                            Err(MVGroupError::Uninitialized)
                        }
                    },
                }
            })
    }

    fn get_latest_group_size(&self, txn_idx: TxnIndex) -> Result<usize, MVGroupError> {
        self.member_tags
            .iter()
            .try_fold(0, |len, tag| {
                match self
                    .versioned_map
                    .get(tag)
                    .expect("Versioned map entry for a member tag must exist")
                    .range(ShiftedTxnIndex::zero()..ShiftedTxnIndex::new(txn_idx))
                    .next_back()
                    .map(|(_, entry)| entry.value.bytes_len())
                {
                    Some(entry_len) => {
                        let delta = entry_len + bcs::serialized_size(tag)?;
                        Ok(len + delta)
                    },
                    None => Ok(len),
                }
            })
            .map_err(|_: anyhow::Error| MVGroupError::TagSerializationError)
    }
}

impl<
        K: Hash + Clone + Debug + Eq,
        T: Hash + Clone + Debug + Eq + Serialize,
        V: TransactionWrite,
    > VersionedGroupData<K, T, V>
{
    pub(crate) fn new() -> Self {
        Self {
            group_values: DashMap::new(),
        }
    }

    pub fn provide_base_values(&self, key: K, base_values: impl IntoIterator<Item = (T, V)>) {
        // Incarnation is irrelevant for storage version, set to 0.
        self.group_values
            .entry(key)
            .or_default()
            .write(ShiftedTxnIndex::zero(), 0, base_values);
    }

    pub fn write(
        &self,
        key: K,
        txn_idx: TxnIndex,
        incarnation: Incarnation,
        values: impl IntoIterator<Item = (T, V)>,
    ) {
        self.group_values.entry(key).or_default().write(
            ShiftedTxnIndex::new(txn_idx),
            incarnation,
            values,
        );
    }

    /// Mark all entry from transaction 'txn_idx' at access path 'key' as an estimated write
    /// (for future incarnation). Will panic if the entry is not in the data-structure.
    pub fn mark_estimate(&self, key: &K, txn_idx: TxnIndex) {
        self.group_values
            .get_mut(key)
            .expect("Path must exist")
            .mark_estimate(txn_idx);
    }

    /// Delete all entries from transaction 'txn_idx' at access path 'key'. Will panic
    /// if the corresponding entry does not exist.
    pub fn delete(&self, key: &K, txn_idx: TxnIndex) {
        self.group_values
            .get_mut(key)
            .expect("Path must exist")
            .delete(txn_idx);
    }

    /// Read the latest value corresponding to a tag at a given group (identified by key).
    /// Return the size of the group (if requested), as defined above, alongside the version
    /// information (None if storage/pre-block version).
    pub fn read_from_group(
        &self,
        key: &K,
        txn_idx: TxnIndex,
        tag: &T,
        return_group_size: bool,
    ) -> anyhow::Result<(Arc<V>, Option<usize>, Version), MVGroupError> {
        match self.group_values.get(key) {
            Some(g) => match g.get_latest_tagged_value(txn_idx, tag) {
                Ok((val, version)) => {
                    let maybe_size = if return_group_size {
                        Some(g.get_latest_group_size(txn_idx)?)
                    } else {
                        None
                    };
                    Ok((val, maybe_size, version))
                },
                Err(e) => Err(e),
            },
            None => Err(MVGroupError::Uninitialized),
        }
    }

    /// Returns the sum of latest sizes of all group members (and their respective tags),
    /// collected based on the list of recorded tags.
    pub fn get_group_size(&self, key: &K, txn_idx: TxnIndex) -> Result<usize, MVGroupError> {
        match self.group_values.get(key) {
            Some(g) => g.get_latest_group_size(txn_idx),
            None => Err(MVGroupError::Uninitialized),
        }
    }

    /// For a given key that corresponds to a group, and an index of a transaction the last
    /// incarnation of which wrote to at least one tag of the group, finalizes the latest
    /// contents of the group. This method works on pointers only and is relatively lighweight,
    /// while subsequent post-processing can clone and serialize the whole group. Note: required
    /// since the output of the block executor still needs to return the whole group contents.
    ///
    /// The method must be called when all transactions <= txn_idx are actually committed, and
    /// the values pointed by weak are guaranteed to be fixed and available during the lifetime
    /// of the data-structure itself.
    pub fn commit_group(&self, key: &K, txn_idx: TxnIndex) -> HashMap<T, Arc<V>> {
        let mut v = self.group_values.get_mut(key).expect("Path must exist");

        v.commit_idx(ShiftedTxnIndex::new(txn_idx))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::types::test::{KeyType, TestValue};
    use test_case::test_case;

    #[should_panic]
    #[test_case(0)]
    #[test_case(1)]
    #[test_case(2)]
    fn test_no_path_exists(test_idx: usize) {
        let ap = KeyType(b"/foo/b".to_vec());
        let map = VersionedGroupData::<KeyType<Vec<u8>>, usize, TestValue>::new();

        match test_idx {
            0 => {
                map.mark_estimate(&ap, 1);
            },
            1 => {
                map.delete(&ap, 2);
            },
            2 => {
                map.commit_group(&ap, 0);
            },
            _ => unreachable!("Wrong test index"),
        }
    }

    // Unitialized;
    // Shifting indexes and logic.
    // read / write / estimate from the other tests.
    // group size.
    // commit group (latest).
    // proptests.
}
