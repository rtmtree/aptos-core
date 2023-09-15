// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_aggregator::delta_change_set::DeltaOp;
use aptos_crypto::hash::HashValue;
use aptos_types::executable::ExecutableDescriptor;
use std::sync::Arc;

pub type TxnIndex = u32;
pub type Incarnation = u32;

/// Custom error type representing storage version. Result<Index, StorageVersion>
/// then represents either index of some type (i.e. TxnIndex, Version), or a
/// version corresponding to the storage (pre-block) state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageVersion;

pub type Version = Result<(TxnIndex, Incarnation), StorageVersion>;

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Flag {
    Done,
    Estimate,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MVGroupError {
    /// The base group contents are not initialized.
    Uninitialized,
    /// Entry corresponding to the tag was not found.
    TagNotFound,
    /// A dependency on other transaction has been found during the read.
    Dependency(TxnIndex),
    /// Tag serialization is needed for group size computation
    TagSerializationError,
}

/// Returned as Err(..) when failed to read from the multi-version data-structure.
#[derive(Debug, PartialEq, Eq)]
pub enum MVDataError {
    /// No prior entry is found.
    Uninitialized,
    /// Read resulted in an unresolved delta value.
    Unresolved(DeltaOp),
    /// A dependency on other transaction has been found during the read.
    Dependency(TxnIndex),
    /// Delta application failed, txn execution should fail.
    DeltaApplicationFailure,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MVModulesError {
    /// No prior entry is found.
    NotFound,
    /// A dependency on other transaction has been found during the read.
    Dependency(TxnIndex),
}

/// Returned as Ok(..) when read successfully from the multi-version data-structure.
#[derive(Debug, PartialEq, Eq)]
pub enum MVDataOutput<V> {
    /// Result of resolved delta op, always u128. Unlike with `Version`, we return
    /// actual data because u128 is cheap to copy and validation can be done correctly
    /// on values as well (ABA is not a problem).
    Resolved(u128),
    /// Information from the last versioned-write. Note that the version is returned
    /// and not the data to avoid copying big values around.
    Versioned(Version, Arc<V>),
}

/// Returned as Ok(..) when read successfully from the multi-version data-structure.
#[derive(Debug, PartialEq, Eq)]
pub enum MVModulesOutput<M, X> {
    /// Arc to the executable corresponding to the latest module, and a descriptor
    /// with either the module hash or indicator that the module is from storage.
    Executable((Arc<X>, ExecutableDescriptor)),
    /// Arc to the latest module, together with its (cryptographic) hash. Note that
    /// this can't be a storage-level module, as it's from multi-versioned modules map.
    /// The Option can be None if HashValue can't be computed, currently may happen
    /// if the latest entry corresponded to the module deletion.
    Module((Arc<M>, HashValue)),
}

// In order to store base vales at the lowest index, i.e. at index 0, without conflicting
// with actual transaction index 0, the following struct wraps the index and internally
// increments it by 1.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) struct ShiftedTxnIndex {
    idx: TxnIndex,
}

impl ShiftedTxnIndex {
    pub fn new(real_idx: TxnIndex) -> Self {
        Self { idx: real_idx + 1 }
    }

    pub(crate) fn idx(&self) -> Result<TxnIndex, StorageVersion> {
        (self.idx > 0).then_some(self.idx - 1).ok_or(StorageVersion)
    }

    pub(crate) fn zero() -> Self {
        Self { idx: 0 }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use aptos_aggregator::delta_change_set::serialize;
    use aptos_types::{
        access_path::AccessPath, executable::ModulePath, state_store::state_value::StateValue,
        write_set::TransactionWrite,
    };
    use bytes::Bytes;
    use std::{fmt::Debug, hash::Hash, sync::Arc};

    #[derive(Clone, Eq, Hash, PartialEq, Debug)]
    pub(crate) struct KeyType<K: Hash + Clone + Debug + Eq>(
        /// Wrapping the types used for testing to add ModulePath trait implementation.
        pub K,
    );

    impl<K: Hash + Clone + Eq + Debug> ModulePath for KeyType<K> {
        fn module_path(&self) -> Option<AccessPath> {
            None
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    pub(crate) struct TestValue {
        bytes: Bytes,
    }

    impl TestValue {
        pub(crate) fn empty() -> Self {
            Self {
                bytes: vec![].into(),
            }
        }

        pub fn new(mut seed: Vec<u32>) -> Self {
            seed.resize(4, 0);
            Self {
                bytes: seed.into_iter().flat_map(|v| v.to_be_bytes()).collect(),
            }
        }

        pub(crate) fn from_u128(value: u128) -> Self {
            Self {
                bytes: serialize(&value).into(),
            }
        }
    }

    impl TransactionWrite for TestValue {
        fn bytes(&self) -> Option<&Bytes> {
            Some(&self.bytes)
        }

        fn from_state_value(_maybe_state_value: Option<StateValue>) -> Self {
            unimplemented!("Irrelevant for the test")
        }

        fn as_state_value(&self) -> Option<StateValue> {
            unimplemented!("Irrelevant for the test")
        }
    }

    // Generate a Vec deterministically based on txn_idx and incarnation.
    pub(crate) fn value_for(txn_idx: TxnIndex, incarnation: Incarnation) -> TestValue {
        TestValue::new(vec![txn_idx * 5, txn_idx + incarnation, incarnation * 5])
    }

    // Generate the value_for txn_idx and incarnation in arc.
    pub(crate) fn arc_value_for(txn_idx: TxnIndex, incarnation: Incarnation) -> Arc<TestValue> {
        // Generate a Vec deterministically based on txn_idx and incarnation.
        Arc::new(value_for(txn_idx, incarnation))
    }

    // Convert value for txn_idx and incarnation into u128.
    pub(crate) fn u128_for(txn_idx: TxnIndex, incarnation: Incarnation) -> u128 {
        value_for(txn_idx, incarnation).as_u128().unwrap().unwrap()
    }
}
