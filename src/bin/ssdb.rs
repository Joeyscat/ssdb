use serde_derive::Deserialize;
use ssdb::{error::Result, storage, Server};

#[tokio::main]
async fn main() -> Result<()> {
    let cfg = Config::new("/etc/ssdb.toml")?;

    let log_level = cfg.log_level.parse::<simplelog::LevelFilter>()?;
    let mut log_config = simplelog::ConfigBuilder::new();
    if log_level != simplelog::LevelFilter::Debug {
        log_config.add_filter_allow_str("ssdb");
    }
    simplelog::SimpleLogger::init(log_level, log_config.build())?;

    let store = storage::kv::StdMemory::new();
    Server::new(Box::new(store))
        .await?
        .listen(&cfg.listen)
        .await?
        .serve()
        .await
}

#[derive(Debug, Deserialize)]
struct Config {
    listen: String,
    log_level: String,
    data_dir: String,
    storage: String,
}

impl Config {
    fn new(file: &str) -> Result<Self> {
        let c = config::Config::builder()
            .set_default("listen", "0.0.0.0:6666")?
            .set_default("log_level", "info")?
            .set_default("data_dir", "/var/lib/ssdb")?
            .set_default("storage", "memory")?
            .add_source(config::File::new(file, config::FileFormat::Toml))
            .build()?;

        Ok(c.try_deserialize()?)
    }
}
