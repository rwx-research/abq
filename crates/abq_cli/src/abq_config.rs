use std::{path::{PathBuf}, fs};
use anyhow::anyhow;

use abq_hosted::AccessToken;
use serde_derive::{Deserialize, Serialize};
use etcetera::{app_strategy, AppStrategy, AppStrategyArgs};

#[derive(Deserialize, Serialize)]
pub struct AbqConfig {
  pub rwx_access_token: AccessToken,
}

fn abq_config_filepath() -> anyhow::Result<PathBuf> {
    let strategy = app_strategy::Unix::new(AppStrategyArgs {
        top_level_domain: "org".to_string(),
        author: "rwx".to_string(),
        app_name: "abq".to_string(),
    })?;
    let config_dir = strategy.config_dir();
    Ok(config_dir.join("config.toml"))
}

pub fn write_abq_config(config: AbqConfig) -> anyhow::Result<PathBuf> {
    let abq_config_filepath = abq_config_filepath()?;
    let config_dir = abq_config_filepath.parent().ok_or(anyhow!("abq config file must have parent dir"))?;
    fs::create_dir_all(config_dir)?;
    let toml_str = toml::to_string(&config)?;
    fs::write(&abq_config_filepath, toml_str)?;
    Ok(abq_config_filepath)
}

pub fn read_abq_config() -> anyhow::Result<AbqConfig> {
    let toml_str = fs::read_to_string(abq_config_filepath()?)?;
    let config = toml::from_str(&toml_str)?;
    Ok(config)
}
