use anyhow::Context;
use config::{Config, Environment, File};
use serde::{Deserialize, de::DeserializeOwned};
use std::{
    env,
    path::{Path, PathBuf},
};

/// Get the command-line argument at position `pos`
pub fn take_from_args(pos: usize) -> Option<String> {
    env::args().nth(pos)
}

/// Load a model config from a file
pub fn load_cfg<T: for<'a> Deserialize<'a>>(path: impl AsRef<Path>) -> anyhow::Result<T> {
    let pb = path.as_ref().to_path_buf();
    if !pb.exists() {
        return Err(anyhow::anyhow!("file {} does not exist", pb.display()));
    }

    let cfg = Config::builder()
        .add_source(config::File::from(PathBuf::from(path.as_ref())))
        .build()
        .with_context(|| format!("failed to read config from {}", pb.display()))?;

    let des: T = cfg
        .try_deserialize()
        .with_context(|| format!("failed to deserialize config from {}", pb.display()))?;

    Ok(des)
}

/// Load a model config from a files
/// If a file does not exist, it is skipped
pub fn load_cfg_merge<T, P>(
    paths: impl IntoIterator<Item = P>,
    env_prefix: Option<&str>,
) -> anyhow::Result<T>
where
    T: DeserializeOwned,
    P: AsRef<Path>,
{
    let mut builder = Config::builder();

    for p in paths {
        let pb = p.as_ref().to_path_buf();
        if pb.exists() {
            builder = builder.add_source(File::from(pb));
        } else {
            println!("config loading: file {} does not exist", pb.display());
        }
    }

    builder = match env_prefix {
        Some(prefix) => builder.add_source(Environment::with_prefix(prefix).separator("__")),
        None => builder.add_source(Environment::default().separator("__")),
    };

    let cfg = builder
        .build()
        .with_context(|| "failed to build configuration from provided sources")?;

    let des: T = cfg
        .try_deserialize()
        .with_context(|| "failed to deserialize merged configuration")?;

    Ok(des)
}
