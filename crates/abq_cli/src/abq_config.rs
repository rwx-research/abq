use anyhow::anyhow;
use std::{fs, path::PathBuf};

use abq_hosted::AccessToken;
use etcetera::{app_strategy, AppStrategy, AppStrategyArgs};
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, PartialEq, Eq)]
pub struct AbqConfig {
    pub rwx_access_token: AccessToken,
}

pub enum AbqConfigFileMode {
    /// Conventional unix location (e.g ~/.config/abq/config.toml)
    Conventional,
    /// Non-standard location (e.g. ~/my/custom/path/config.toml)
    Override(String),
    /// Ignore any located config file (For instance, if the host machine happens to have a config file)
    Ignore,
}

pub fn abq_config_filepath(mode: AbqConfigFileMode) -> Option<PathBuf> {
    match mode {
        AbqConfigFileMode::Ignore => None,
        AbqConfigFileMode::Override(path) => Some(PathBuf::from(path)),
        AbqConfigFileMode::Conventional => {
            let strategy = app_strategy::Unix::new(AppStrategyArgs {
                top_level_domain: "org".to_string(),
                author: "rwx".to_string(),
                app_name: "abq".to_string(),
            });
            let config_dir =
                Result::expect(strategy, "failed to build conventional abq config filepath")
                    .config_dir();
            Some(config_dir.join("config.toml"))
        }
    }
}

pub fn write_abq_config(
    config: AbqConfig,
    config_path: anyhow::Result<PathBuf>,
) -> anyhow::Result<PathBuf> {
    let abq_config_filepath = config_path?;
    let config_dir = abq_config_filepath
        .parent()
        .ok_or_else(|| anyhow!("abq config file must have parent dir"))?;
    fs::create_dir_all(config_dir)?;
    let toml_str = toml::to_string(&config)?;
    fs::write(&abq_config_filepath, toml_str)?;
    Ok(abq_config_filepath)
}

pub fn read_abq_config(path: Option<PathBuf>) -> Option<AbqConfig> {
    let path = path?;
    let toml_str = fs::read_to_string(path).ok()?;
    toml::from_str(&toml_str).ok()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_abq_filepath_explicit_none() {
        assert_eq!(abq_config_filepath(AbqConfigFileMode::Ignore), None);
    }

    #[test]
    fn test_abq_filepath_explicit_some() {
        assert_eq!(
            abq_config_filepath(AbqConfigFileMode::Override("~/my/custom/path".to_string())),
            Some(PathBuf::from("~/my/custom/path"))
        );
    }

    #[test]
    fn test_abq_filepath_implicit_none() {
        let config_path = abq_config_filepath(AbqConfigFileMode::Conventional).unwrap();
        let suffix = ".abq/config.toml";
        assert!(config_path.as_path().ends_with(suffix));
    }

    #[test]
    fn test_abq_config_file_missing_returns_none() {
        let read_config = read_abq_config(Some("/derp/does_not_exist/config.toml".into()));
        assert!(read_config.is_none());
    }

    #[test]
    fn test_abq_config_operations() {
        let access_token = AccessToken::from_str("testy_mctesterson").unwrap();
        // Create a temporary directory to store the config file
        let tmp_config_file = NamedTempFile::new().expect("Failed to create temporary file");

        // Set up the test configuration
        let config = AbqConfig {
            rwx_access_token: access_token,
        };

        // Write the config file
        let config_path = write_abq_config(config, Ok(tmp_config_file.path().to_path_buf()));

        // Read the config file
        let read_config = read_abq_config(Some(config_path.unwrap()));
        assert!(read_config.is_some());

        assert_eq!(
            read_config.unwrap().rwx_access_token.to_string(),
            "testy_mctesterson"
        );
    }
}
