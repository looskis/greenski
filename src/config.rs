use anyhow::{Context, Result};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

const DEFAULT_PORT: u16 = 8789;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub port: u16,
    pub webhook_url: Option<String>,
    pub hmac_secret: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            webhook_url: None,
            hmac_secret: random_secret(),
        }
    }
}

impl Config {
    pub fn load_or_init() -> Result<Self> {
        let dir = config_dir();
        fs::create_dir_all(&dir)
            .with_context(|| format!("create configuration directory {}", dir.display()))?;
        secure_directory(&dir)?;

        let path = config_path();
        if path.exists() {
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("read configuration {}", path.display()))?;
            return toml::from_str(&raw)
                .with_context(|| format!("parse configuration {}", path.display()));
        }

        let config = Self::default();
        fs::write(&path, toml::to_string_pretty(&config)?)
            .with_context(|| format!("write configuration {}", path.display()))?;
        secure_file(&path)?;
        Ok(config)
    }
}

pub fn config_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join("greenski")
}

pub fn config_path() -> PathBuf {
    config_dir().join("config.toml")
}

pub fn whatsapp_db_path() -> PathBuf {
    config_dir().join("whatsapp.db")
}

pub fn state_db_path() -> PathBuf {
    config_dir().join("state.db")
}

pub fn pid_path() -> PathBuf {
    config_dir().join("daemon.pid")
}

pub fn lock_path() -> PathBuf {
    config_dir().join("daemon.lock")
}

pub fn stdout_log_path() -> PathBuf {
    config_dir().join("greenski.log")
}

pub fn stderr_log_path() -> PathBuf {
    config_dir().join("greenski.err.log")
}

pub fn launch_agent_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("Library")
        .join("LaunchAgents")
        .join("com.looskis.greenski.plist")
}

fn random_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(unix)]
fn secure_directory(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn secure_directory(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
pub fn secure_file(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
pub fn secure_file(_path: &std::path::Path) -> Result<()> {
    Ok(())
}
