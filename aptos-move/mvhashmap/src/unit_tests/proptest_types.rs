// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{
    types::{test::KeyType, MVDataError, MVDataOutput, TxnIndex},
    MVHashMap,
};
use aptos_aggregator::delta_change_set::{delta_add, delta_sub, DeltaOp};
use aptos_types::{
    executable::ExecutableTestType, state_store::state_value::StateValue,
    write_set::TransactionWrite,
};
use bytes::Bytes;
use claims::assert_none;
use proptest::{collection::vec, prelude::*, sample::Index, strategy::Strategy};
use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    hash::Hash,
    sync::atomic::{AtomicUsize, Ordering},
};

const DEFAULT_TIMEOUT: u64 = 30;

#[derive(Debug, Clone)]
enum Operator<V: Debug + Clone> {
    Insert(V),
    Remove,
    Read,
    Update(DeltaOp),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExpectedOutput<V: Debug + Clone + PartialEq> {
    NotInMap,
    Deleted,
    Value(V),
    Resolved(u128),
    Unresolved(DeltaOp),
    Failure,
}

struct Value<V> {
    maybe_value: Option<V>,
    maybe_bytes: Option<Bytes>,
}

impl<V: Into<Vec<u8>> + Clone> Value<V> {
    fn new(maybe_value: Option<V>) -> Self {
        let maybe_bytes = maybe_value.clone().map(|v| {
            let mut bytes = v.into();
            bytes.resize(16, 0);
            bytes.into()
        });
        Self {
            maybe_value,
            maybe_bytes,
        }
    }
}

impl<V: Into<Vec<u8>> + Clone> TransactionWrite for Value<V> {
    fn bytes(&self) -> Option<&Bytes> {
        self.maybe_bytes.as_ref()
    }

    fn from_state_value(_maybe_state_value: Option<StateValue>) -> Self {
        unimplemented!("Irrelevant for the test")
    }

    fn as_state_value(&self) -> Option<StateValue> {
        unimplemented!("Irrelevant for the test")
    }
}

enum Data<V> {
    Write(Value<V>),
    Delta(DeltaOp),
}
struct Baseline<K, V>(HashMap<K, BTreeMap<TxnIndex, Data<V>>>);

impl<K, V> Baseline<K, V>
where
    K: Hash + Eq + Clone + Debug,
    V: Clone + Into<Vec<u8>> + Debug + PartialEq,
{
    pub fn new(txns: &[(K, Operator<V>)]) -> Self {
        let mut baseline: HashMap<K, BTreeMap<TxnIndex, Data<V>>> = HashMap::new();
        for (idx, (k, op)) in txns.iter().enumerate() {
            let value_to_update = match op {
                Operator::Insert(v) => Data::Write(Value::new(Some(v.clone()))),
                Operator::Remove => Data::Write(Value::new(None)),
                Operator::Update(d) => Data::Delta(*d),
                Operator::Read => continue,
            };

            baseline
                .entry(k.clone())
                .or_insert_with(BTreeMap::new)
                .insert(idx as TxnIndex, value_to_update);
        }
        Self(baseline)
    }

    pub fn get(&self, key: &K, txn_idx: TxnIndex) -> ExpectedOutput<V> {
        match self.0.get(key).map(|tree| tree.range(..txn_idx)) {
            None => ExpectedOutput::NotInMap,
            Some(mut iter) => {
                let mut acc: Option<DeltaOp> = None;
                let mut failure = false;
                while let Some((_, data)) = iter.next_back() {
                    match data {
                        Data::Write(v) => match acc {
                            Some(d) => {
                                match v.as_u128().unwrap() {
                                    Some(value) => {
                                        assert!(!failure); // acc should be none.
                                        match d.apply_to(value) {
                                            Err(_) => return ExpectedOutput::Failure,
                                            Ok(i) => return ExpectedOutput::Resolved(i),
                                        }
                                    },
                                    None => {
                                        // v must be a deletion.
                                        assert_none!(v.bytes());
                                        return ExpectedOutput::Deleted;
                                    },
                                }
                            },
                            None => match v.maybe_value.as_ref() {
                                Some(w) => {
                                    return if failure {
                                        ExpectedOutput::Failure
                                    } else {
                                        ExpectedOutput::Value(w.clone())
                                    };
                                },
                                None => return ExpectedOutput::Deleted,
                            },
                        },
                        Data::Delta(d) => match acc.as_mut() {
                            Some(a) => {
                                if a.merge_with_previous_delta(*d).is_err() {
                                    failure = true;
                                }
                            },
                            None => acc = Some(*d),
                        },
                    }

                    if failure {
                        // for overriding the delta failure if entry is deleted.
                        acc = None;
                    }
                }

                if failure {
                    ExpectedOutput::Failure
                } else {
                    match acc {
                        Some(d) => ExpectedOutput::Unresolved(d),
                        None => ExpectedOutput::NotInMap,
                    }
                }
            },
        }
    }
}

fn operator_strategy<V: Arbitrary + Clone>() -> impl Strategy<Value = Operator<V>> {
    prop_oneof![
        2 => any::<V>().prop_map(Operator::Insert),
        4 => any::<u32>().prop_map(|v| {
            // TODO: Is there a proptest way of doing that?
            if v % 2 == 0 {
                Operator::Update(delta_sub(v as u128, u32::MAX as u128))
            } else {
        Operator::Update(delta_add(v as u128, u32::MAX as u128))
            }
        }),
        1 => Just(Operator::Remove),
        1 => Just(Operator::Read),
    ]
}

fn run_and_assert<K, V>(
    universe: Vec<K>,
    transaction_gens: Vec<(Index, Operator<V>)>,
) -> Result<(), TestCaseError>
where
    K: PartialOrd + Send + Clone + Hash + Eq + Sync + Debug,
    V: Send + Into<Vec<u8>> + Debug + Clone + PartialEq + Sync,
{
    let transactions: Vec<(K, Operator<V>)> = transaction_gens
        .into_iter()
        .map(|(idx, op)| (idx.get(&universe).clone(), op))
        .collect::<Vec<_>>();

    let baseline = Baseline::new(transactions.as_slice());
    // Only testing data, provide executable type ().
    let map = MVHashMap::<KeyType<K>, usize, Value<V>, ExecutableTestType>::new();

    // make ESTIMATE placeholders for all versions to be updated.
    // allows to test that correct values appear at the end of concurrent execution.
    let versions_to_write = transactions
        .iter()
        .enumerate()
        .filter_map(|(idx, (key, op))| match op {
            Operator::Read => None,
            Operator::Insert(_) | Operator::Remove | Operator::Update(_) => {
                Some((key.clone(), idx))
            },
        })
        .collect::<Vec<_>>();
    for (key, idx) in versions_to_write {
        map.data()
            .write(KeyType(key.clone()), idx as TxnIndex, 0, Value::new(None));
        map.data().mark_estimate(&KeyType(key), idx as TxnIndex);
    }

    let current_idx = AtomicUsize::new(0);

    // Spawn a few threads in parallel to commit each operator.
    rayon::scope(|s| {
        for _ in 0..universe.len() {
            s.spawn(|_| loop {
                // Each thread will eagerly fetch an Operator to execute.
                let idx = current_idx.fetch_add(1, Ordering::Relaxed);
                if idx >= transactions.len() {
                    // Abort when all transactions are processed.
                    break;
                }
                let key = &transactions[idx].0;
                match &transactions[idx].1 {
                    Operator::Read => {
                        use MVDataError::*;
                        use MVDataOutput::*;

                        let baseline = baseline.get(key, idx as TxnIndex);
                        let mut retry_attempts = 0;
                        loop {
                            match map
                                .data()
                                .fetch_data(&KeyType(key.clone()), idx as TxnIndex)
                            {
                                Ok(Versioned(_, v)) => {
                                    match v.maybe_value.as_ref() {
                                        Some(w) => {
                                            assert_eq!(
                                                baseline,
                                                ExpectedOutput::Value(w.clone()),
                                                "{:?}",
                                                idx
                                            );
                                        },
                                        None => {
                                            assert_eq!(
                                                baseline,
                                                ExpectedOutput::Deleted,
                                                "{:?}",
                                                idx
                                            );
                                        },
                                    }
                                    break;
                                },
                                Ok(Resolved(v)) => {
                                    assert_eq!(baseline, ExpectedOutput::Resolved(v), "{:?}", idx);
                                    break;
                                },
                                Err(Uninitialized) => {
                                    assert_eq!(baseline, ExpectedOutput::NotInMap, "{:?}", idx);
                                    break;
                                },
                                Err(DeltaApplicationFailure) => {
                                    assert_eq!(baseline, ExpectedOutput::Failure, "{:?}", idx);
                                    break;
                                },
                                Err(Unresolved(d)) => {
                                    assert_eq!(
                                        baseline,
                                        ExpectedOutput::Unresolved(d),
                                        "{:?}",
                                        idx
                                    );
                                    break;
                                },
                                Err(Dependency(_i)) => (),
                            }
                            retry_attempts += 1;
                            if retry_attempts > DEFAULT_TIMEOUT {
                                panic!("Failed to get value for {:?}", idx);
                            }
                            std::thread::sleep(std::time::Duration::from_millis(100));
                        }
                    },
                    Operator::Remove => {
                        map.data().write(
                            KeyType(key.clone()),
                            idx as TxnIndex,
                            1,
                            Value::new(None),
                        );
                    },
                    Operator::Insert(v) => {
                        map.data().write(
                            KeyType(key.clone()),
                            idx as TxnIndex,
                            1,
                            Value::new(Some(v.clone())),
                        );
                    },
                    Operator::Update(delta) => {
                        map.data()
                            .add_delta(KeyType(key.clone()), idx as TxnIndex, *delta)
                    },
                }
            })
        }
    });
    Ok(())
}

// TODO: proptest MVHashMap delete and dependency handling!

proptest! {
    #[test]
    fn single_key_proptest(
        universe in vec(any::<[u8; 32]>(), 1),
        transactions in vec((any::<Index>(), operator_strategy::<[u8; 32]>()), 100),
    ) {
        run_and_assert(universe, transactions)?;
    }

    #[test]
    fn single_key_large_transactions(
        universe in vec(any::<[u8; 32]>(), 1),
        transactions in vec((any::<Index>(), operator_strategy::<[u8; 32]>()), 2000),
    ) {
        run_and_assert(universe, transactions)?;
    }

    #[test]
    fn multi_key_proptest(
        universe in vec(any::<[u8; 32]>(), 10),
        transactions in vec((any::<Index>(), operator_strategy::<[u8; 32]>()), 100),
    ) {
        run_and_assert(universe, transactions)?;
    }
}
