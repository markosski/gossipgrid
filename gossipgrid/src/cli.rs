
use clap::{Parser, Command};

const WEB_PORT_DEFAULT: &str = "3001";
const NODE_PORT_DEFAULT: &str = "4109";

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct NodeCliArgs {
    #[arg(short, long)]
    name: String,

    #[arg(short, long, default_value_t = 1)]
    count: u8,
}

pub fn node_cli() -> Command {
    Command::new("gossipgrid")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(
            Command::new("cluster")
                .about("Start a cluster with specified size")
                .arg(
                    clap::Arg::new("size")
                        .short('s')
                        .long("size")
                        .value_name("SIZE")
                        .help("Size of the cluster")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("replication-factor")
                        .short('r')
                        .long("replication-factor")
                        .value_name("REPLICATION_FACTOR")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("partition-size")
                        .short('p')
                        .long("partition-size")
                        .value_name("PARTITION_SIZE")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("web-port")
                        .short('w')
                        .long("web-port")
                        .value_name("WEB_PORT")
                        .default_value(WEB_PORT_DEFAULT),
                )
                .arg(
                    clap::Arg::new("node-port")
                        .short('n')
                        .long("node-port")
                        .value_name("NODE_PORT")
                        .default_value(NODE_PORT_DEFAULT),
                ),
        )
        .subcommand(
            Command::new("join")
                .about("Join an existing cluster")
                .arg(
                    clap::Arg::new("address")
                        .short('a')
                        .long("address")
                        .value_name("ADDRESS")
                        .help("Network address of the peer node to join")
                        .required(true),
                )
                .arg(
                    clap::Arg::new("web-port")
                        .short('w')
                        .long("web-port")
                        .value_name("WEB_PORT")
                        .default_value(WEB_PORT_DEFAULT),
                )
                .arg(
                    clap::Arg::new("node-port")
                        .short('n')
                        .long("node-port")
                        .value_name("NODE_PORT")
                        .default_value(NODE_PORT_DEFAULT),
                ),
        )
}