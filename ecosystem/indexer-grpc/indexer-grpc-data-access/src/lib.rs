// Copyright © Aptos Foundation

use serde::{Deserialize, Serialize};

pub mod access_trait;
pub mod gcs;
pub mod in_memory;
pub mod in_memory_storage;
pub mod local_file;
pub mod redis;

use crate::access_trait::{
    AccessMetadata, StorageReadError, StorageReadStatus, StorageTransactionRead,
};

#[enum_dispatch::enum_dispatch]
#[derive(Clone)]
pub enum StorageClient {
    InMemory(in_memory::InMemoryStorageClient),
    Redis(redis::RedisClient),
    GCS(gcs::GcsClient),
    LocalFile(local_file::LocalFileClient),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "storage_type")]
pub enum ReadOnlyStorageType {
    InMemory(in_memory::InMemoryStorageClientConfig),
    Redis(redis::RedisClientConfig),
    GCS(gcs::GcsClientConfig),
    LocalFile(local_file::LocalFileClientConfig),
}

const REDIS_ENDING_VERSION_EXCLUSIVE_KEY: &str = "latest_version";
const REDIS_CHAIN_ID: &str = "chain_id";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
struct FileMetadata {
    pub chain_id: u64,
    pub file_folder_size: u64,
    pub version: u64,
}

type Based64EncodedSerializedTransactionProtobuf = String;

#[derive(Debug, Serialize, Deserialize)]
struct TransactionsFile {
    pub transactions: Vec<Based64EncodedSerializedTransactionProtobuf>,
    pub starting_version: u64,
}

#[inline]
fn get_transactions_file_name(version: u64) -> String {
    // This assumes that the transactions are stored in file of 1000 versions.
    format!("files/{}.json", version / 1000 * 1000)
}
