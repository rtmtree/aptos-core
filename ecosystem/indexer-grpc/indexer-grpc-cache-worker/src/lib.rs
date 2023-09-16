// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub mod metrics;
pub mod worker;

use anyhow::{anyhow, bail, Ok, Result};
use aptos_indexer_grpc_server_framework::RunnableConfig;
use aptos_indexer_grpc_utils::config::IndexerGrpcFileStoreConfig;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};
use worker::Worker;

pub const SERVER_NAME: &str = "idxcache";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcCacheWorkerConfig {
    pub fullnode_grpc_address: String,
    pub file_store_config: IndexerGrpcFileStoreConfig,
    pub redis_main_instance_address: String,
}

#[async_trait::async_trait]
impl RunnableConfig for IndexerGrpcCacheWorkerConfig {
    async fn run(&self) -> Result<()> {
        let mut worker = Worker::new(
            self.fullnode_grpc_address.clone(),
            self.redis_main_instance_address.clone(),
            self.file_store_config.clone(),
        )
        .await;
        worker.run().await;
        Ok(())
    }

    fn get_server_name(&self) -> String {
        SERVER_NAME.to_string()
    }
}

// todo consider putting this stuff in another file

impl IndexerGrpcCacheWorkerConfig {
    // todo add comments
    fn default_test_config() -> Self {
        Self {
            fullnode_grpc_address: "127.0.0.1:50051".to_string(),
            file_store_config: IndexerGrpcFileStoreConfig::default(),
            redis_main_instance_address: "127.0.0.1:6379".to_string(),
        }
    }

    // todo add comment for this and args
    pub fn create_test_config(
        config_override_path: Option<PathBuf>,
        fullnode_grpc_address: Option<String>,
        local_file_store_path: Option<PathBuf>,
        redis_main_instance_address: Option<String>,
    ) -> Result<IndexerGrpcCacheWorkerConfig> {
        let mut config = match config_override_path {
            // If a config override path was provided, merge it with the default config
            Some(test_config_override_path) => {
                let file = fs::File::open(&test_config_override_path).map_err(|e| {
                    anyhow!(
                        "Unable to open config override file {:?}. Error: {}",
                        test_config_override_path,
                        e
                    )
                })?;
                let override_values: serde_yaml::Value =
                    serde_yaml::from_reader(&file).map_err(|e| {
                        anyhow!(
                            "Unable to read config override file as YAML {:?}. Error: {}",
                            test_config_override_path,
                            e
                        )
                    })?;
                merge_config(
                    IndexerGrpcCacheWorkerConfig::default_test_config(),
                    override_values,
                )?
            },
            None => IndexerGrpcCacheWorkerConfig::default_test_config(),
        };
        // TODO: Don't allow people to include override values for the things we change
        // below here, otherwise it might confuse users ^^^

        // Apply any changes to the config that we have to make regardless of what the
        // user set in the overrides.

        // Update the fullnode address if given.
        if let Some(fullnode_grpc_address) = fullnode_grpc_address {
            config.fullnode_grpc_address = fullnode_grpc_address;
        }

        // Update the local file store path if given.
        if let Some(local_file_store_path) = local_file_store_path {
            match &mut config.file_store_config {
                IndexerGrpcFileStoreConfig::GcsFileStore(_) => {
                    bail!("Cannot use the local_file_store_path argument with a GCS file store")
                },
                IndexerGrpcFileStoreConfig::LocalFileStore(ref mut store) => {
                    store.local_file_store_path = local_file_store_path;
                },
            }
        }

        // Update the redis address if given.
        if let Some(redis_main_instance_address) = redis_main_instance_address {
            config.redis_main_instance_address = redis_main_instance_address;
        }

        Ok(config)
    }
}

/// Merges a config with override values.
pub fn merge_config<T: serde::de::DeserializeOwned + serde::Serialize>(
    base_config: T,
    override_values: serde_yaml::Value,
) -> Result<T> {
    serde_merge::tmerge::<T, serde_yaml::Value, T>(base_config, override_values).map_err(|e| {
        anyhow!(
            "Unable to merge default config with override. Error: {:#}",
            e
        )
    })
}
