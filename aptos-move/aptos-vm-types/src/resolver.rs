// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_aggregator::{aggregator_extension::AggregatorID, resolver::TAggregatorView};
use aptos_state_view::StateViewId;
use aptos_types::state_store::{
    state_key::StateKey,
    state_storage_usage::StateStorageUsage,
    state_value::{StateValue, StateValueMetadataKind},
};
use bytes::Bytes;
use move_core_types::value::MoveTypeLayout;

/// Allows to query resources from the state.
pub trait TResourceView {
    type Key;
    type Layout;

    /// Returns
    ///   -  Ok(None)         if the resource is not in storage,
    ///   -  Ok(Some(...))    if the resource exists in storage,
    ///   -  Err(...)         otherwise (e.g. storage error).
    fn get_resource_state_value(
        &self,
        key: &Self::Key,
        maybe_layout: Option<&Self::Layout>,
    ) -> anyhow::Result<Option<StateValue>>;

    fn get_resource_bytes(
        &self,
        key: &Self::Key,
        maybe_layout: Option<&Self::Layout>,
    ) -> anyhow::Result<Option<Bytes>> {
        let maybe_state_value = self.get_resource_state_value(key, maybe_layout)?;
        Ok(maybe_state_value.map(|state_value| state_value.bytes().clone()))
    }

    fn get_resource_state_value_metadata(
        &self,
        key: &Self::Key,
    ) -> anyhow::Result<Option<StateValueMetadataKind>> {
        // For metadata, layouts are not important.
        let maybe_state_value = self.get_resource_state_value(key, None)?;
        Ok(maybe_state_value.map(StateValue::into_metadata))
    }

    fn resource_exists(&self, key: &Self::Key) -> anyhow::Result<bool> {
        // For existence, layouts are not important.
        self.get_resource_state_value(key, None)
            .map(|maybe_state_value| maybe_state_value.is_some())
    }
}

pub trait TResourceGroupView {
    type Key;
    type Tag;

    fn get_resource_from_group(
        &self,
        _key: &Self::Key,
        _resource_tag: &Self::Tag,
        _return_group_size: bool,
    ) -> anyhow::Result<(Option<Bytes>, Option<u64>)> {
        unimplemented!("TResourceGroupView not yet implemented");
    }

    /// Implements the functionality requested by get_resource_group_state_value_metadata
    /// from StateValueMetadataResolver, which on top of StateValueMetadataKind, requires
    /// a speculative size of the resource group before the transaction.
    fn get_resource_group_state_value_metadata(
        &self,
        _state_key: &Self::Key,
    ) -> anyhow::Result<Option<(StateValueMetadataKind, u64)>> {
        unimplemented!("TResourceGroupView not yet implemented");
    }

    fn resource_group_exists(&self, _key: &Self::Key) -> anyhow::Result<bool> {
        unimplemented!("TResourceGroupView not yet implemented");
    }

    /// Needed for backwards compatibility with the additional safety mechanism for resource
    /// groups, where the violation of the following invariant causes transaction failure:
    /// - if a resource is modified or deleted it must already exist within a group,
    /// and if it is created, it must not previously exist.
    ///
    /// For normal resources, this is asserted, but for resource groups the behavior (that
    /// we maintain) is for the transaction to fail with INVARIANT_VIOLATION_ERROR.
    /// This ensures state does not change and blockchain does not halt while the underlying
    /// issue is addressed. In order to maintain the behavior we check for resource existence,
    /// which in the context of parallel execution does not cause a full R/W conflict.
    ///
    /// Note: If and when we start using the method in other use-cases, in particular, if it
    /// may access a resource group for the first time, we should also incorporate the size
    /// charge for such access.
    fn resource_exists_in_group(
        &self,
        _key: &Self::Key,
        _resource_tag: &Self::Tag,
    ) -> anyhow::Result<bool> {
        unimplemented!("TResourceGroupView not yet implemented");
    }
}

/// Allows to query modules from the state.
pub trait TModuleView {
    type Key;

    /// Returns
    ///   -  Ok(None)         if the module is not in storage,
    ///   -  Ok(Some(...))    if the module exists in storage,
    ///   -  Err(...)         otherwise (e.g. storage error).
    fn get_module_state_value(&self, key: &Self::Key) -> anyhow::Result<Option<StateValue>>;

    fn get_module_bytes(&self, key: &Self::Key) -> anyhow::Result<Option<Bytes>> {
        let maybe_state_value = self.get_module_state_value(key)?;
        Ok(maybe_state_value.map(|state_value| state_value.bytes().clone()))
    }

    fn get_module_state_value_metadata(
        &self,
        key: &Self::Key,
    ) -> anyhow::Result<Option<StateValueMetadataKind>> {
        let maybe_state_value = self.get_module_state_value(key)?;
        Ok(maybe_state_value.map(StateValue::into_metadata))
    }

    fn module_exists(&self, key: &Self::Key) -> anyhow::Result<bool> {
        self.get_module_state_value(key)
            .map(|maybe_state_value| maybe_state_value.is_some())
    }
}

/// Allows to query state information, e.g. its usage.
pub trait StateStorageView {
    fn id(&self) -> StateViewId;

    fn get_usage(&self) -> anyhow::Result<StateStorageUsage>;
}

/// A fine-grained view of the state during execution.
///
/// - The `StateView` trait should be used by the storage backend, e.g. a DB.
///   It only allows a generic key-value access and always returns bytes or
///   state values.
/// - The `ExecutorView` trait is used at executor level, e.g. BlockSTM. When
///   a block is executed, the types of accesses are always known (for example,
///   whether a resource is accessed or a module). Fine-grained structure of
///   `ExecutorView` allows to:
///     1. Specialize on access type,
///     2. Separate execution and storage abstractions.
///
///  **WARNING:** There is no default implementation of `ExecutorView` for
///  `StateView` in order to ensure that a correct type is always used. If
///   conversion from state to executor view is needed, an adapter can be used.
pub trait TExecutorView<K, L, I>:
    TResourceView<Key = K, Layout = L>
    // + TResourceGroupView<Key = K, Tag = T>
    + TModuleView<Key = K>
    + TAggregatorView<IdentifierV1 = K, IdentifierV2 = I>
    + StateStorageView
{
}

impl<A, K, L, I> TExecutorView<K, L, I> for A where
    A: TResourceView<Key = K, Layout = L>
        // + TResourceGroupView<Key = K, Tag = T>
        + TModuleView<Key = K>
        + TAggregatorView<IdentifierV1 = K, IdentifierV2 = I>
        + StateStorageView
{
}

pub trait ExecutorView: TExecutorView<StateKey, MoveTypeLayout, AggregatorID> {}

impl<T> ExecutorView for T where T: TExecutorView<StateKey, MoveTypeLayout, AggregatorID> {}

/// Allows to query storage metadata in the VM session. Needed for storage refunds.
/// - Result being Err means storage error or some incostistency (e.g. during speculation,
/// needing to abort/halt the transaction with an error status).
/// - Ok(None) means that the corresponding data does not exist / was deleted.
/// - Ok(Some(_ : MetadataKind)) may be internally None (within Kind) if the metadata was
/// not previously provided (e.g. Legacy WriteOps).
pub trait StateValueMetadataResolver {
    fn get_module_state_value_metadata(
        &self,
        state_key: &StateKey,
    ) -> anyhow::Result<Option<StateValueMetadataKind>>;

    fn get_resource_state_value_metadata(
        &self,
        state_key: &StateKey,
    ) -> anyhow::Result<Option<StateValueMetadataKind>>;

    /// u64 provided alongside StateValueMetadataKind is a (speculative) size of the
    /// group before the transaction. The size is collected based on atomic reads, but
    /// is only guaranteed to correspond to the correct size if executing on a quiescent,
    /// correct state. The provided value can be viewed as a branch prediction in the
    /// parallel executor setting, as a wrong value will be (later) caught by validation.
    ///
    /// This size is used to compute the size of the resource group at the end of the
    /// transaction, required for storage gas fee charging. Thus, in parallel execution,
    /// this will lead to a deterministic estimate (based on a validated state) of the
    /// value that can be used for the gas purposes. Thus, R/W conflicts are avoided
    /// in parallel execution, as long as the estimates are correct (e.g. updating struct
    /// members of a fixed size).
    fn get_resource_group_state_value_metadata(
        &self,
        state_key: &StateKey,
    ) -> anyhow::Result<Option<(StateValueMetadataKind, u64)>>;
}
