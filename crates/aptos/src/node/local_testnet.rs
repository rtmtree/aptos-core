// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    common::{
        types::{CliCommand, CliError, CliTypedResult, ConfigSearchMode, PromptOptions},
        utils::prompt_yes_with_override,
    },
    config::GlobalConfig,
};
use anyhow::Context;
use aptos_config::config::NodeConfig;
use aptos_faucet_core::server::{FunderKeyEnum, RunConfig as FaucetConfig};
use aptos_indexer_grpc_cache_worker::IndexerGrpcCacheWorkerConfig;
use aptos_indexer_grpc_data_service::{IndexerGrpcDataServiceConfig, NonTlsConfig};
use aptos_indexer_grpc_file_store::IndexerGrpcFileStoreWorkerConfig;
use aptos_indexer_grpc_server_framework::{run_server_with_config, setup_logging, GenericConfig};
use aptos_indexer_grpc_utils::config::IndexerGrpcFileStoreConfig;
use aptos_logger::debug;
use aptos_node::create_single_node_test_config;
use async_trait::async_trait;
use clap::Parser;
use futures::{Future, FutureExt};
use processor::processors::Processor;
use rand::{rngs::StdRng, SeedableRng};
use reqwest::Url;
use std::{
    fs::{create_dir_all, remove_dir_all, File},
    net::SocketAddr,
    path::PathBuf,
    process::Stdio,
    thread,
    time::Duration,
};
use tokio::{process::Command, task::JoinHandle, time::Instant};
use tracing_subscriber::fmt::MakeWriter;

// What if we made the data service just use redis only for local testnets? they're
// generally short lived so is the filestore stuff really necessary?

const MAX_WAIT_S: u64 = 30;
const WAIT_INTERVAL_MS: u64 = 100;
// TODO: Consider renaming this to localnet. Might not be worth the effort.
const TESTNET_FOLDER: &str = "testnet";

/// Run local testnet
///
/// This local testnet will run it's own Genesis and run as a single node
/// network locally.  Optionally, a faucet can be added for minting APT coins.
#[derive(Parser)]
pub struct RunLocalTestnet {
    // todo explain the difference between config path and test config override better
    /// An overridable config template for the test node
    ///
    /// If provided, the config will be used, and any needed configuration for the local testnet
    /// will override the config's values
    #[clap(long, value_parser)]
    config_path: Option<PathBuf>,

    /// The directory to save all files for the node
    ///
    /// Defaults to .aptos/testnet
    #[clap(long, value_parser)]
    test_dir: Option<PathBuf>,

    /// Path to node configuration file override for local test mode.
    ///
    /// If provided, the default node config will be overridden by the config in the given file.
    /// Cannot be used with --config-path
    #[clap(long, value_parser, conflicts_with("config_path"))]
    test_config_override: Option<PathBuf>,

    /// Random seed for key generation in test mode
    ///
    /// This allows you to have deterministic keys for testing
    #[clap(long, value_parser = aptos_node::load_seed)]
    seed: Option<[u8; 32]>,

    /// Clean the state and start with a new chain at genesis
    ///
    /// This will wipe the aptosdb in `test-dir` to remove any incompatible changes, and start
    /// the chain fresh.  Note, that you will need to publish the module again and distribute funds
    /// from the faucet accordingly
    #[clap(long)]
    force_restart: bool,

    /// Do not run a faucet alongside the node
    ///
    /// Allows you to run a faucet alongside the node to create and fund accounts for testing
    #[clap(long)]
    no_faucet: bool,

    /// Port to run the faucet on
    ///
    /// When running, you'll be able to use the faucet at `http://127.0.0.1:<port>/mint` e.g.
    /// `http//127.0.0.1:8081/mint`
    #[clap(long, default_value_t = 8081)]
    faucet_port: u16,

    // TODO: Should I make this more granular? Maybe not to start with, as long as
    // there are args that say where to run each service from.
    /// Do not run a txn stream service.
    #[clap(long)]
    no_txn_stream: bool,

    /// Do not run the standard set of indexer processors.
    #[clap(long)]
    no_indexer_processors: bool,

    // TODO: Before continuing with this we probably want the thingo that lets you run
    // a single connection to the txn stream and then multiple processors use that.
    // https://stackoverflow.com/questions/77113537/how-to-specify-the-default-value-for-a-vector-argument-with-clap-4
    /// Processors to run. If set, only these processors will be run.
    #[clap(
        long,
        value_enum,
        default_values_t = vec![
            Processor::CoinProcessor,
            Processor::DefaultProcessor,
            Processor::FungibleAssetProcessor,
            Processor::StakeProcessor,
            Processor::TokenV2Processor
        ]
    )]
    processors: Vec<Processor>,

    /// Disable the delegation of faucet minting to a dedicated account
    #[clap(long)]
    do_not_delegate: bool,

    #[clap(flatten)]
    prompt_options: PromptOptions,
}

// There needs to be a variant of this that has enums for each component. For each enum
// there need to be options like "run using built in library", "run using binary at
// this path", "run using docker", "use this instance of the component that is running
// here", or "don't run at all".
#[derive(Debug)]
struct AllConfigs {
    node_config: NodeConfig,
    faucet_config: Option<FaucetConfig>,
    txn_stream_configs: Option<TxnStreamConfigs>,
}

impl AllConfigs {
    /// Return all the ports that we'll try to kill before starting the testnet.
    pub fn get_ports(&self) -> Vec<u16> {
        let mut ports = Vec::new();
        ports.push(self.node_config.api.address.port());
        if let Some(faucet_config) = &self.faucet_config {
            ports.push(faucet_config.server_config.listen_port);
        }
        if let Some(txn_stream_configs) = &self.txn_stream_configs {
            // TODO: Use the configurable ports.
            ports.push(txn_stream_configs.cache_worker_config.health_check_port);
            ports.push(
                txn_stream_configs
                    .file_store_service_config
                    .health_check_port,
            );
            ports.push(txn_stream_configs.data_service_config.health_check_port);
            ports.push(6379);
        }
        ports
    }
}

#[derive(Debug)]
struct TxnStreamConfigs {
    cache_worker_config: GenericConfig<IndexerGrpcCacheWorkerConfig>,
    file_store_service_config: GenericConfig<IndexerGrpcFileStoreWorkerConfig>,
    data_service_config: GenericConfig<IndexerGrpcDataServiceConfig>,
}

impl RunLocalTestnet {
    // Config building phase and then use that to figure out what to run.
    fn build_configs(&self, test_dir: PathBuf) -> anyhow::Result<AllConfigs> {
        let rng = self
            .seed
            .map(StdRng::from_seed)
            .unwrap_or_else(StdRng::from_entropy);

        let mut node_config = create_single_node_test_config(
            &self.config_path,
            &self.test_config_override,
            &test_dir,
            false,
            false,
            aptos_cached_packages::head_release_bundle(),
            rng,
        )
        .context("Failed to create config for node")?;

        // Enable the grpc stream on the node if we will run a txn stream service.
        node_config.indexer_grpc.enabled = !self.no_txn_stream;

        // TODO: Without this the conversion logic will fail. Our config sanitizer
        // needs to catch this. See more here: https://gist.github.com/banool/b6dfb020d709cbf58c2babf96f005dd9
        node_config.storage.enable_indexer = true;

        let node_api_url = socket_addr_to_url(&node_config.api.address, "http")
            .context("Failed to parse API listen address as URL")?;

        let faucet_config = if self.no_faucet {
            None
        } else {
            Some(FaucetConfig::build_for_cli(
                node_api_url.clone(),
                self.faucet_port,
                FunderKeyEnum::KeyFile(test_dir.join("mint.key")),
                self.do_not_delegate,
                None,
            ))
        };

        let txn_stream_configs = if self.no_txn_stream {
            None
        } else {
            let cache_worker_config = IndexerGrpcCacheWorkerConfig::create_test_config(
                None,
                // TODO make this configurable:
                None,
                Some(test_dir.join(aptos_indexer_grpc_cache_worker::SERVER_NAME)),
                // TODO make this configurable
                Some("127.0.0.1:6379".to_string()),
            )
            .context("Failed to create config for txn stream service cache worker")?;

            let file_store_service_config = IndexerGrpcFileStoreWorkerConfig {
                file_store_config: cache_worker_config.file_store_config.clone(),
                redis_main_instance_address: cache_worker_config
                    .redis_main_instance_address
                    .clone(),
            };

            let data_service_config = IndexerGrpcDataServiceConfig {
                data_service_grpc_tls_config: None,
                data_service_grpc_non_tls_config: Some(NonTlsConfig {
                    data_service_grpc_listen_address: "0.0.0.0:50052".to_string(),
                }),
                data_service_response_channel_size: None,
                whitelisted_auth_tokens: vec![],
                disable_auth_check: true,
                file_store_config: cache_worker_config.file_store_config.clone(),
                redis_read_replica_address: cache_worker_config.redis_main_instance_address.clone(),
            };

            Some(TxnStreamConfigs {
                cache_worker_config: GenericConfig {
                    server_config: cache_worker_config,
                    health_check_port: 55550,
                },
                file_store_service_config: GenericConfig {
                    server_config: file_store_service_config,
                    health_check_port: 55551,
                },
                data_service_config: GenericConfig {
                    server_config: data_service_config,
                    health_check_port: 55552,
                },
            })
        };

        // TODO: Validate that there is no config overlap, e.g. with ports.

        Ok(AllConfigs {
            node_config,
            faucet_config,
            txn_stream_configs,
        })
    }

    /// todo
    async fn start_node(
        &self,
        test_dir: PathBuf,
        config: NodeConfig,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        let rng = self
            .seed
            .map(StdRng::from_seed)
            .unwrap_or_else(StdRng::from_entropy);

        let node_thread_handle = thread::spawn(move || {
            let result = aptos_node::setup_test_environment_and_start_node(
                None,
                None,
                Some(config),
                Some(test_dir),
                false,
                false,
                aptos_cached_packages::head_release_bundle(),
                rng,
            );
            eprintln!("Node stopped unexpectedly {:#?}", result);
        });

        // This future just waits for the node thread.
        let node_future = async move {
            loop {
                if node_thread_handle.is_finished() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        };

        Ok(node_future)
    }

    async fn start_faucet(
        &self,
        config: FaucetConfig,
        node_api_url: Url,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        wait_for_node_startup(&node_api_url, "faucet").await?;

        // Start the faucet
        Ok(config.run().map(|result| {
            eprintln!("Faucet stopped unexpectedly {:#?}", result);
        }))

        // TODO: Verify that the faucet came up before returning here? That might
        // not work though, we have to return the future for it to start running.

        // TODO: Might be better to not have a bunch of futures but to instead spwan a
        // bunch of stuff on a runtime.

        // TODO: Write docs about how these functions can wait for stuff to happen
        // before returning the future but can't do anything after that point.
    }

    // TODO: Support docker and "don't do anything" options too. Also maybe support
    // absolute path for the binary.
    async fn start_redis(&self, test_dir: &PathBuf) -> CliTypedResult<impl Future<Output = ()>> {
        // TODO: If requested, wipe the DB.

        let redis_dir = test_dir.join("redis");
        create_dir_all(redis_dir.as_path()).map_err(|err| {
            CliError::IO(format!("Failed to create {}", redis_dir.display()), err)
        })?;

        let file = File::create(redis_dir.join("stdout.log")).map_err(|err| {
            CliError::IO(format!("Failed to create {}", redis_dir.display()), err)
        })?;
        let stdio = Stdio::from(file);

        let mut child = Command::new("redis-server")
            .current_dir(redis_dir)
            .stdout(stdio)
            .kill_on_drop(true)
            .spawn()
            .map_err(|err| {
                CliError::UnexpectedError(format!("Failed to start redis-server: {}", err))
            })?;

        let fut = async move {
            match child.wait().await {
                Ok(status) => {
                    eprintln!("Redis server stopped unexpectedly with status: {}", status);
                },
                Err(err) => {
                    eprintln!("Redis server stopped unexpectedly with error: {}", err);
                },
            }
        };

        Ok(fut)
    }

    async fn start_txn_stream_cache_worker(
        &self,
        config: GenericConfig<IndexerGrpcCacheWorkerConfig>,
        node_api_url: &Url,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        // TODO: We could extract the URL from the config too but it's a string
        // right now rather than a Url.
        wait_for_node_startup(node_api_url, "txn-stream-cache-worker").await?;
        wait_for_redis_startup(
            &config.server_config.redis_main_instance_address,
            "txn-stream-cache-worker",
        )
        .await?;

        // Create the local file store directory if it doesn't exist.
        if let IndexerGrpcFileStoreConfig::LocalFileStore(config) =
            &config.server_config.file_store_config
        {
            create_dir_all(config.local_file_store_path.as_path()).map_err(|err| {
                CliError::IO(
                    format!(
                        "Failed to create local file store directory {}",
                        config.local_file_store_path.display()
                    ),
                    err,
                )
            })?;
        };

        Ok(run_server_with_config(config).map(|result| {
            eprintln!(
                "Transaction Stream Service Cache Worker stopped unexpectedly {:#?}",
                result
            );
        }))
    }

    // Despite the name, this is a service that uses file store, not file store itself.
    // Furthermore, it doesn't even use filestore, it either uses local storage or GCS.
    async fn start_txn_stream_file_store(
        &self,
        config: GenericConfig<IndexerGrpcFileStoreWorkerConfig>,
        cache_worker_health_check_url: &Url,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        wait_for_http_startup(&cache_worker_health_check_url, "txn-stream-file-store").await?;

        Ok(run_server_with_config(config).map(|result| {
            eprintln!(
                "Transaction Stream Service File Store Service stopped unexpectedly {:#?}",
                result
            );
        }))
    }

    async fn start_txn_stream_data_service(
        &self,
        config: GenericConfig<IndexerGrpcDataServiceConfig>,
        cache_worker_health_check_url: &Url,
        file_store_service_health_check_url: &Url,
    ) -> CliTypedResult<impl Future<Output = ()>> {
        wait_for_http_startup(&cache_worker_health_check_url, "txn-stream-data-service").await?;
        wait_for_http_startup(
            &file_store_service_health_check_url,
            "txn-stream-data-service",
        )
        .await?;

        Ok(run_server_with_config(config).map(|result| {
            eprintln!(
                "Transaction Stream Service File Store Service stopped unexpectedly {:#?}",
                result
            );
        }))
    }
}

// TODO: Handle where all the logs go. This is sort of like reinventing docker-compose lol.

#[async_trait]
impl CliCommand<()> for RunLocalTestnet {
    fn command_name(&self) -> &'static str {
        "RunLocalTestnet"
    }

    async fn execute(mut self) -> CliTypedResult<()> {
        let global_config = GlobalConfig::load().context("Failed to load global config")?;
        let test_dir = match &self.test_dir {
            Some(test_dir) => test_dir.clone(),
            None => global_config
                .get_config_location(ConfigSearchMode::CurrentDirAndParents)?
                .join(TESTNET_FOLDER),
        };

        // Remove the current test directory and start with a new node
        if test_dir.exists() && self.force_restart {
            prompt_yes_with_override(
                "Are you sure you want to delete the existing chain?",
                self.prompt_options,
            )?;
            remove_dir_all(test_dir.as_path()).map_err(|err| {
                CliError::IO(format!("Failed to delete {}", test_dir.display()), err)
            })?;
        }

        if !test_dir.exists() {
            debug!("Test directory does not exist, creating it: {:?}", test_dir);
            create_dir_all(test_dir.as_path()).map_err(|err| {
                CliError::IO(format!("Failed to create {}", test_dir.display()), err)
            })?;
            debug!("Created test directory: {:?}", test_dir);
        }

        let td = test_dir.clone();
        let make_writer = move || ThreadNameMakeWriter::new(td.clone()).make_writer();
        setup_logging(Some(Box::new(make_writer)));

        let all_configs = self
            .build_configs(test_dir.clone())
            .context("Failed to build configs")?;

        for port in all_configs.get_ports() {
            kill_process_by_port(port).await?;
        }

        let AllConfigs {
            node_config,
            faucet_config,
            txn_stream_configs,
        } = all_configs;

        let node_api_url = socket_addr_to_url(&node_config.api.address, "http")?;

        // TODO: Previously I used a bunch of futures like in the faucet. I think this
        // allows for concurrency even in the face of blocking tasks assuming the tokio
        // runtime is configured properly (which idk if it is).
        let mut tasks: Vec<JoinHandle<()>> = Vec::new();
        tasks.push(tokio::spawn(
            self.start_node(test_dir.clone(), node_config)
                .await
                .context("Failed to create future to start the node")?,
        ));

        if let Some(faucet_config) = faucet_config {
            tasks.push(tokio::spawn(
                self.start_faucet(faucet_config, node_api_url.clone())
                    .await
                    .context("Failed to create future to start the faucet")?,
            ));
        }

        // TODO: Leverage the health check ports to ensure the services are healthy.

        if let Some(txn_stream_configs) = txn_stream_configs {
            let TxnStreamConfigs {
                cache_worker_config,
                file_store_service_config,
                data_service_config,
            } = txn_stream_configs;
            let cache_worker_health_check_url = Url::parse(&format!(
                "http://127.0.0.1:{}",
                cache_worker_config.health_check_port
            ))
            .unwrap();
            let file_store_service_health_check_url = Url::parse(&format!(
                "http://127.0.0.1:{}",
                file_store_service_config.health_check_port
            ))
            .unwrap();
            tasks.push(tokio::spawn(
                self.start_redis(&test_dir)
                    .await
                    .context("Failed to create future to start the Redis server")?,
            ));
            tasks.push(tokio::spawn(
                self.start_txn_stream_cache_worker(cache_worker_config, &node_api_url)
                    .await
                    .context(
                        "Failed to create future to start the txn stream service cache worker",
                    )?,
            ));
            tasks.push(tokio::spawn(
                self.start_txn_stream_file_store(
                    file_store_service_config,
                    &cache_worker_health_check_url,
                )
                .await
                .context("Failed to create future to start the txn stream file store service")?,
            ));
            tasks.push(tokio::spawn(
                self.start_txn_stream_data_service(
                    data_service_config,
                    &cache_worker_health_check_url,
                    &file_store_service_health_check_url,
                )
                .await
                .context("Failed to create future to start the txn stream file store service")?,
            ));
        }

        // Wait for all the futures. We should never get past this point unless
        // something goes wrong or the user signals for the process to end.
        let result = futures::future::select_all(tasks).await;

        Err(CliError::UnexpectedError(format!(
            "One of the components stopped unexpectedly: {:?}",
            result
        )))
    }
}

fn socket_addr_to_url(socket_addr: &SocketAddr, scheme: &str) -> anyhow::Result<Url> {
    let host = match socket_addr {
        SocketAddr::V4(v4) => format!("{}", v4.ip()),
        SocketAddr::V6(v6) => format!("[{}]", v6.ip()),
    };
    let full_url = format!("{}://{}:{}", scheme, host, socket_addr.port());
    Ok(Url::parse(&full_url)?)
}

async fn wait_for_http_startup(url: &Url, service: &str) -> CliTypedResult<()> {
    let f = || async {
        reqwest::get(url.clone())
            .await
            .with_context(|| format!("Failed to GET {}", url))
            .map(|_| ())
    };
    wait_for_startup(
        f,
        format!("Server at {} did not start up before {}", url, service),
    )
    .await
}

async fn wait_for_node_startup(node_api_url: &Url, service: &str) -> CliTypedResult<()> {
    let f = || async {
        aptos_rest_client::Client::new(node_api_url.clone())
            .get_index()
            .await?;
        Ok(())
    };
    wait_for_startup(
        f,
        format!(
            "Node API at {} did not start up before {}",
            node_api_url, service
        ),
    )
    .await
}

// TODO: Ideally we make the cache worker just keep trying for a few minutes on
// whenever it can't talk to Redis, at startup or otherwise. Then we don't
// have to do this.
async fn wait_for_redis_startup(redis_address: &String, service: &str) -> CliTypedResult<()> {
    let redis_address = format!("redis://{}", redis_address);
    let f = || async {
        let redis_address = redis_address.clone();
        let redis_client = redis::Client::open(redis_address.clone()).with_context(|| {
            format!(
                "Failed to create Redis client for address {}",
                redis_address
            )
        })?;
        redis_client
            .get_tokio_connection_manager()
            .await
            .with_context(|| format!("Failed to connect to Redis at address {}", redis_address))?;
        Ok(())
    };
    wait_for_startup(
        f,
        format!(
            "Redis at {} did not start up before {}",
            redis_address, service
        ),
    )
    .await
}

async fn wait_for_startup<F, Fut>(check_fn: F, error_message: String) -> CliTypedResult<()>
where
    F: Fn() -> Fut,
    Fut: futures::Future<Output = anyhow::Result<()>>,
{
    let max_wait = Duration::from_secs(MAX_WAIT_S);
    let wait_interval = Duration::from_millis(WAIT_INTERVAL_MS);

    let start = Instant::now();
    let mut started_successfully = false;

    while start.elapsed() < max_wait {
        if check_fn().await.is_ok() {
            started_successfully = true;
            break;
        }
        tokio::time::sleep(wait_interval).await
    }

    if !started_successfully {
        return Err(CliError::UnexpectedError(error_message));
    }

    Ok(())
}

// TODO: Wait so the cache worker stores in filestore and redis. We don't store in
// Redis and then pull out of it with a separate process. Make sure the diagram
// reflects this. Then what does indexer-grpc-file-store do?

// Actually that's not true ^ the cache worker indeed only stores in Redis. It does
// read from filestore though to see where we're up to. Makes me wonder yet again
// about whether we can get away with having only Redis.

async fn get_pid_by_port(port: u16) -> anyhow::Result<Option<String>> {
    let output = Command::new("lsof")
        .arg("-i")
        .arg(format!(":{}", port))
        .arg("-t")
        .output()
        .await
        .context("Failed to execute lsof")?;

    let pid = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if pid.is_empty() {
        Ok(None)
    } else {
        Ok(Some(pid))
    }
}

async fn kill_process_by_pid(pid: String) -> anyhow::Result<()> {
    let _ = Command::new("kill")
        .arg("-9")
        .arg(&pid)
        .output()
        .await
        .with_context(|| format!("Failed to kill process with PID {}", pid))?;
    Ok(())
}

async fn kill_process_by_port(port: u16) -> anyhow::Result<()> {
    if let Some(pid) = get_pid_by_port(port).await? {
        kill_process_by_pid(pid)
            .await
            .with_context(|| format!("Failed to kill process at port {}", port))?;
        eprintln!("Killed existing process at port {}", port);
    }
    Ok(())
}

// TODO: Set up tracing logging.
// todo: Add tests for both --force-restart and not --force-restart

/*
fn make_my_great_writer() -> Box<dyn std::io::Write> {
    let thread_name = std::thread::current().name().unwrap_or("other").to_string();
    // Keep everything up until the last dash.
    let file_name = thread_name.split('-').collect::<Vec<_>>().join("-");
    let log_file = File::create(format!("/tmp/{}.log", thread_name)).unwrap();
    Box::new(log_file)
}
*/

/// TODO: Explain
pub struct ThreadNameMakeWriter {
    test_dir: PathBuf,
}

impl ThreadNameMakeWriter {
    pub fn new(test_dir: PathBuf) -> Self {
        Self { test_dir }
    }
}

impl<'a> MakeWriter<'a> for ThreadNameMakeWriter {
    type Writer = Box<dyn std::io::Write>;

    // TODO: I think this runs every time we write a log line, not ideal. Make we can
    // memoize whether the dir / file exists?
    fn make_writer(&'a self) -> Self::Writer {
        let thread_name = std::thread::current()
            .name()
            .unwrap_or("no-thread-name")
            .to_string();
        let thread_name_no_number = truncate_last_segment(&thread_name, '-');
        let dir_path = self.test_dir.join(thread_name_no_number);
        create_dir_all(&dir_path).expect("Failed to create log directory");
        let log_path = dir_path.join("tracing.log");
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .unwrap();
        Box::new(log_file)
    }
}

fn truncate_last_segment(s: &str, delimiter: char) -> String {
    s.rsplitn(2, delimiter).nth(1).unwrap_or(s).to_string()
}
