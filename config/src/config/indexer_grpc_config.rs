// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::config::{
    config_sanitizer::ConfigSanitizer, node_config_loader::NodeType, Error, NodeConfig,
};
use anyhow::Context;
use aptos_types::chain_id::ChainId;
use serde::{Deserialize, Serialize};
use std::net::ToSocketAddrs;

// Useful indexer defaults
const DEFAULT_ADDRESS: &str = "0.0.0.0:50051";
const DEFAULT_OUTPUT_BATCH_SIZE: u16 = 100;
const DEFAULT_PROCESSOR_BATCH_SIZE: u16 = 1000;
const DEFAULT_PROCESSOR_TASK_COUNT: u16 = 20;

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerGrpcConfig {
    pub enabled: bool,

    /// The address that the grpc server will listen on
    #[serde(default = "IndexerGrpcConfig::default_address")]
    pub address: String,

    /// Number of processor tasks to fan out
    #[serde(default = "IndexerGrpcConfig::default_processor_task_count")]
    pub processor_task_count: u16,

    /// Number of transactions each processor will process
    #[serde(default = "IndexerGrpcConfig::default_processor_batch_size")]
    pub processor_batch_size: u16,

    /// Number of transactions returned in a single stream response
    #[serde(default = "IndexerGrpcConfig::default_output_batch_size")]
    pub output_batch_size: u16,
}

impl IndexerGrpcConfig {
    fn default_address() -> String {
        DEFAULT_ADDRESS.into()
    }

    fn default_processor_task_count() -> u16 {
        DEFAULT_PROCESSOR_TASK_COUNT
    }

    fn default_processor_batch_size() -> u16 {
        DEFAULT_PROCESSOR_BATCH_SIZE
    }

    fn default_output_batch_size() -> u16 {
        DEFAULT_OUTPUT_BATCH_SIZE
    }
}

// I see, so the reason we have all this default handling in the ConfigSanitizer is
// because we don't even use the Default or serde::default stuff, even if it is here,
// because we start with a config from a file, at least in the testing case...
// what a mess, why do we define it in a file... we should should just use Config::default()
// and then make changes to it in code.
// https://aptos-org.slack.com/archives/C03F71PU3B5/p1694699525458539
impl ConfigSanitizer for IndexerGrpcConfig {
    fn sanitize(
        node_config: &mut NodeConfig,
        _node_type: NodeType,
        _chain_id: ChainId,
    ) -> Result<(), Error> {
        node_config.indexer_grpc.address = DEFAULT_ADDRESS.into();
        node_config.indexer_grpc.processor_task_count = DEFAULT_PROCESSOR_TASK_COUNT;
        node_config.indexer_grpc.processor_batch_size = DEFAULT_PROCESSOR_BATCH_SIZE;
        node_config.indexer_grpc.output_batch_size = DEFAULT_OUTPUT_BATCH_SIZE;

        // TODO I don't want to have to do any of that ^^^^^
        // https://aptos-org.slack.com/archives/C03F71PU3B5/p1694699525458539

        // Make sure the address can be converted to SocketAddrs.
        node_config
            .indexer_grpc
            .address
            .to_socket_addrs()
            .with_context(||
                format!(
                    "Listen address for indexer GRPC node stream ({}) could not be parsed as SocketAddrs",
                    node_config
                        .indexer_grpc
                        .address
                )
            )?
            .next()
            .with_context(||
                format!(
                "Failed to parse indexer GRPC node stream ({}) listen address",
                node_config
                    .indexer_grpc
                    .address
                )
            )?;
        Ok(())
    }
}
