use anyhow::{Context, Result};
use clap::{App, Arg, SubCommand};
use gw_block_producer::replay::{replay_block, replay_chain, ReplayError};
use gw_block_producer::{db_block_validator, runner};
use gw_config::Config;
use gw_version::Version;
use std::fs::{create_dir_all, write};
use std::path::PathBuf;
use std::{fs, path::Path};

const COMMAND_RUN: &str = "run";
const COMMAND_EXAMPLE_CONFIG: &str = "generate-example-config";
const COMMAND_VERIFY_DB_BLOCK: &str = "verify-db-block";
const COMMAND_REPLAY_BLOCK: &str = "replay-block";
const COMMAND_REPLAY_CHAIN: &str = "replay-chain";
const ARG_OUTPUT_PATH: &str = "output-path";
const ARG_CONFIG: &str = "config";
const ARG_SKIP_CONFIG_CHECK: &str = "skip-config-check";
const ARG_FROM_BLOCK: &str = "from-block";
const ARG_TO_BLOCK: &str = "to-block";

fn read_config<P: AsRef<Path>>(path: P) -> Result<Config> {
    let content = fs::read(&path)
        .with_context(|| format!("read config file from {}", path.as_ref().to_string_lossy()))?;
    let config = toml::from_slice(&content).with_context(|| "parse config file")?;
    Ok(config)
}

fn generate_example_config<P: AsRef<Path>>(path: P) -> Result<()> {
    let mut config = Config::default();
    config.backends.push(Default::default());
    config.block_producer = Some(Default::default());
    let content = toml::to_string_pretty(&config)?;
    fs::write(path, content)?;
    Ok(())
}

fn run_cli() -> Result<()> {
    let version = Version::current().to_string();
    let app = App::new("Godwoken")
        .about("The layer2 rollup built upon Nervos CKB.")
        .version(version.as_ref())
        .subcommand(
            SubCommand::with_name(COMMAND_RUN)
                .about("Run Godwoken node")
                .arg(
                    Arg::with_name(ARG_CONFIG)
                        .short("c")
                        .takes_value(true)
                        .required(true)
                        .default_value("./config.toml")
                        .help("The config file path"),
                )
                .arg(
                    Arg::with_name(ARG_SKIP_CONFIG_CHECK)
                        .long(ARG_SKIP_CONFIG_CHECK)
                        .help("Force to accept unsafe config file"),
                )
                .display_order(0),
        )
        .subcommand(
            SubCommand::with_name(COMMAND_EXAMPLE_CONFIG)
                .about("Generate an example config file")
                .arg(
                    Arg::with_name(ARG_OUTPUT_PATH)
                        .short("o")
                        .takes_value(true)
                        .required(true)
                        .default_value("./config.example.toml")
                        .help("The path of the example config file"),
                )
                .display_order(1),
        )
        .subcommand(
            SubCommand::with_name(COMMAND_VERIFY_DB_BLOCK)
                .about("Verify history blocks in db")
                .arg(
                    Arg::with_name(ARG_CONFIG)
                        .short("c")
                        .takes_value(true)
                        .required(true)
                        .default_value("./config.toml")
                        .help("The config file path"),
                )
                .arg(
                    Arg::with_name(ARG_FROM_BLOCK)
                        .short("f")
                        .takes_value(true)
                        .help("From block number"),
                )
                .arg(
                    Arg::with_name(ARG_TO_BLOCK)
                        .short("t")
                        .takes_value(true)
                        .help("To block number"),
                )
                .display_order(2),
        )
        .subcommand(
            SubCommand::with_name(COMMAND_REPLAY_BLOCK)
                .about("Replay a db block")
                .arg(
                    Arg::with_name(ARG_CONFIG)
                        .short("c")
                        .takes_value(true)
                        .required(true)
                        .default_value("./config.toml")
                        .help("The config file path"),
                )
                .arg(
                    Arg::with_name("block-number")
                        .short("b")
                        .takes_value(true)
                        .required(true)
                        .help("block number"),
                )
                .display_order(3),
        )
        .subcommand(
            SubCommand::with_name(COMMAND_REPLAY_CHAIN)
                .about("Replay chain")
                .arg(
                    Arg::with_name(ARG_CONFIG)
                        .short("c")
                        .takes_value(true)
                        .required(true)
                        .default_value("./config.toml")
                        .help("The config file path"),
                )
                .arg(
                    Arg::with_name("dst-store")
                        .short("s")
                        .takes_value(true)
                        .required(true)
                        .help("The dst replay chain"),
                )
                .arg(
                    Arg::with_name("ARG_FROM_BLOCK")
                        .short("f")
                        .takes_value(true)
                        .help("From block number"),
                )
                .display_order(4),
        );

    // handle subcommands
    let matches = app.clone().get_matches();
    match matches.subcommand() {
        (COMMAND_RUN, Some(m)) => {
            let config_path = m.value_of(ARG_CONFIG).unwrap();
            let config = read_config(&config_path)?;
            runner::run(config, m.is_present(ARG_SKIP_CONFIG_CHECK))?;
        }
        (COMMAND_EXAMPLE_CONFIG, Some(m)) => {
            let path = m.value_of(ARG_OUTPUT_PATH).unwrap();
            generate_example_config(path)?;
        }
        (COMMAND_VERIFY_DB_BLOCK, Some(m)) => {
            let config_path = m.value_of(ARG_CONFIG).unwrap();
            let config = read_config(&config_path)?;
            let from_block: Option<u64> = m.value_of(ARG_FROM_BLOCK).map(str::parse).transpose()?;
            let to_block: Option<u64> = m.value_of(ARG_TO_BLOCK).map(str::parse).transpose()?;
            db_block_validator::verify(config, from_block, to_block)?;
        }
        (COMMAND_REPLAY_BLOCK, Some(m)) => {
            let config_path = m.value_of(ARG_CONFIG).unwrap();
            let config = read_config(&config_path)?;
            let block_number: u64 = m
                .value_of("block-number")
                .map(str::parse)
                .transpose()?
                .expect("block number");
            if let Err(ReplayError::State(state)) = replay_block(&config, block_number) {
                let json_state = serde_json::to_string_pretty(&state)?;
                let dir = config.debug.debug_tx_dump_path.as_path();
                create_dir_all(&dir)?;

                let mut dump_path = PathBuf::new();
                dump_path.push(dir);

                let dump_filename = format!("{}-{}-state.json", state.block_number, state.tx_hash);
                dump_path.push(dump_filename);

                write(dump_path, json_state)?;
            }
        }
        (COMMAND_REPLAY_CHAIN, Some(m)) => {
            let config_path = m.value_of(ARG_CONFIG).unwrap();
            let config = read_config(&config_path)?;
            let from_block: Option<u64> = m.value_of(ARG_FROM_BLOCK).map(str::parse).transpose()?;
            let dst_store = m.value_of("dst-store").unwrap();
            replay_chain(&config, dst_store, from_block)?;
        }
        _ => {
            // default command: start a Godwoken node
            let config_path = "./config.toml";
            let config = read_config(&config_path)?;
            runner::run(config, false)?;
        }
    };
    Ok(())
}

/// Godwoken entry
fn main() {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    run_cli().expect("run cli");
}
