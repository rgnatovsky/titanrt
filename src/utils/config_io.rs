use anyhow::Context;
use config::Config;
use serde::Deserialize;
use std::path::PathBuf;

pub fn load_cfg<T: for<'a> Deserialize<'a>>(path: impl AsRef<str>) -> anyhow::Result<T> {
    let cfg = Config::builder()
        .add_source(config::File::from(PathBuf::from(path.as_ref())))
        .build()
        .with_context(|| format!("failed to read model config from {}", path.as_ref()))?;

    let model: T = cfg
        .try_deserialize()
        .with_context(|| format!("failed to deserialize model config from {}", path.as_ref()))?;

    Ok(model)
}
