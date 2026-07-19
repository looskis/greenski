use crate::config::{self, Config};
use anyhow::{Context, Result, bail};
use fs2::FileExt;
use qrcode::QrCode;
use qrcode::render::unicode;
use serde_json::{Value, json};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

pub struct DaemonGuard {
    _lock: File,
}

impl DaemonGuard {
    pub fn acquire() -> Result<Self> {
        fs::create_dir_all(config::config_dir())?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(config::lock_path())?;
        lock.try_lock_exclusive()
            .context("another Greenski daemon is already running")?;
        fs::write(config::pid_path(), std::process::id().to_string())?;
        config::secure_file(&config::pid_path())?;
        Ok(Self { _lock: lock })
    }
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(config::pid_path());
    }
}

pub fn send(
    to: Option<String>,
    text: Option<String>,
    protocol: String,
    client_ref: Option<String>,
    positional: Vec<String>,
) -> Result<()> {
    let to = to.or_else(|| positional.first().cloned());
    let text = text.or_else(|| positional.get(1).cloned());
    let (Some(to), Some(text)) = (to, text) else {
        bail!("usage: greenski send --to <number> --text <message>");
    };
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let response = http_client()?
        .post(format!("http://127.0.0.1:{}/messages", config.port))
        .json(&json!({
            "to": to,
            "text": text,
            "protocol": protocol,
            "client_ref": client_ref,
        }))
        .send()?
        .error_for_status()?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response.json::<Value>()?)?
    );
    Ok(())
}

pub fn events(since: i64, limit: Option<u64>, follow: bool) -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    if follow {
        let stream_client = reqwest::blocking::Client::builder()
            .connect_timeout(Duration::from_millis(500))
            .build()?;
        let mut response = stream_client
            .get(format!(
                "http://127.0.0.1:{}/events/stream?since={since}",
                config.port
            ))
            .send()?
            .error_for_status()?;
        let mut reader = BufReader::new(&mut response);
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            print!("{line}");
            std::io::stdout().flush()?;
        }
        return Ok(());
    }

    let mut url = format!("http://127.0.0.1:{}/events?since={since}", config.port);
    if let Some(limit) = limit {
        url.push_str(&format!("&limit={limit}"));
    }
    let events = http_client()?
        .get(url)
        .send()?
        .error_for_status()?
        .json::<Vec<Value>>()?;
    for event in events {
        println!("{}", serde_json::to_string(&event)?);
    }
    Ok(())
}

pub fn chats(limit: u64) -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let chats = http_client()?
        .get(format!("http://127.0.0.1:{}/chats", config.port))
        .query(&[("limit", limit)])
        .send()?
        .error_for_status()?
        .json::<Vec<Value>>()?;
    for chat in chats {
        println!("{}", serde_json::to_string(&chat)?);
    }
    Ok(())
}

pub fn messages(from: String, limit: u64, before: Option<i64>) -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let mut request = http_client()?
        .get(format!("http://127.0.0.1:{}/messages/history", config.port))
        .query(&[("from", from), ("limit", limit.to_string())]);
    if let Some(before) = before {
        request = request.query(&[("before", before)]);
    }
    let messages = request.send()?.error_for_status()?.json::<Vec<Value>>()?;
    for message in messages {
        println!("{}", serde_json::to_string(&message)?);
    }
    Ok(())
}

pub fn sync_history(from: String, count: i32) -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let response = http_client()?
        .post(format!("http://127.0.0.1:{}/history/sync", config.port))
        .json(&json!({ "from": from, "count": count }))
        .send()?
        .error_for_status()?;
    println!(
        "{}",
        serde_json::to_string_pretty(&response.json::<Value>()?)?
    );
    Ok(())
}

pub fn pair() -> Result<()> {
    let config = Config::load_or_init()?;
    ensure_running(&config)?;
    let url = format!("http://127.0.0.1:{}/pairing", config.port);
    let client = http_client()?;
    let mut last_code = None;
    println!("Waiting for a WhatsApp linked-device QR code…");

    loop {
        let value = client
            .get(&url)
            .send()?
            .error_for_status()?
            .json::<Value>()?;
        if value["status"] == "connected" {
            if let Some(account) = value["account"].as_str() {
                println!("Greenski is paired and connected as {account}");
            } else {
                println!("Greenski is paired and connected");
            }
            return Ok(());
        }
        if matches!(value["status"].as_str(), Some("failed" | "logged_out")) {
            let reason = value["last_error"]
                .as_str()
                .unwrap_or("WhatsApp connection failed");
            bail!("{reason}");
        }
        if let Some(code) = value["code"].as_str()
            && last_code.as_deref() != Some(code)
        {
            render_qr(code)?;
            println!("Scan with WhatsApp → Settings → Linked Devices → Link a Device");
            last_code = Some(code.to_string());
        }
        std::thread::sleep(Duration::from_millis(750));
    }
}

fn render_qr(payload: &str) -> Result<()> {
    let code = QrCode::new(payload.as_bytes()).context("render pairing QR code")?;
    let image = code.render::<unicode::Dense1x2>().quiet_zone(true).build();
    println!("{image}");
    Ok(())
}

pub fn up() -> Result<()> {
    let config = Config::load_or_init()?;
    let already_running = probe(&config)?.is_some();
    ensure_running(&config)?;
    println!(
        "greenski {} on 127.0.0.1:{}",
        if already_running { "already up" } else { "up" },
        config.port
    );
    status()
}

pub fn status() -> Result<()> {
    let config = Config::load_or_init()?;
    let Some(value) = probe(&config)? else {
        bail!("Greenski is not running");
    };
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

pub fn ensure_running(config: &Config) -> Result<()> {
    if probe(config)?.is_some() {
        return Ok(());
    }

    let executable = std::env::current_exe()?;
    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::stdout_log_path())?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(config::stderr_log_path())?;
    let mut command = Command::new(executable);
    command
        .arg("run")
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // A daemon started from SSH or another non-interactive shell must not
        // share that shell's session, otherwise logout can deliver SIGHUP.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    command.spawn().context("start Greenski daemon")?;

    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(250));
        if probe(config)?.is_some() {
            return Ok(());
        }
    }
    bail!(
        "daemon did not become healthy; see {}",
        config::stderr_log_path().display()
    )
}

pub fn down() -> Result<()> {
    let pid_path = config::pid_path();
    let pid: i32 = match fs::read_to_string(&pid_path) {
        Ok(value) => value.trim().parse().context("invalid daemon pid file")?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!("Greenski is not running");
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };

    #[cfg(unix)]
    {
        if !pid_belongs_to_greenski(pid)? {
            let _ = fs::remove_file(&pid_path);
            bail!("stale daemon pid file referenced process {pid}; no signal was sent");
        }
        let result = unsafe { libc::kill(pid, libc::SIGTERM) };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error.into());
            }
        }
    }
    #[cfg(not(unix))]
    bail!("greenski down is currently supported on Unix systems");

    let _ = fs::remove_file(pid_path);
    println!("stopped Greenski (pid {pid})");
    Ok(())
}

#[cfg(unix)]
fn pid_belongs_to_greenski(pid: i32) -> Result<bool> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
        .context("inspect daemon process")?;
    if !output.status.success() {
        return Ok(false);
    }
    let command = String::from_utf8_lossy(&output.stdout);
    let executable_name = std::env::current_exe()?
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("greenski")
        .to_string();
    let process_name = command
        .split_whitespace()
        .next()
        .and_then(|path| std::path::Path::new(path).file_name())
        .and_then(|name| name.to_str());
    Ok(process_name == Some(executable_name.as_str()))
}

fn probe(config: &Config) -> Result<Option<Value>> {
    let response = match http_client()?
        .get(format!("http://127.0.0.1:{}/status", config.port))
        .send()
    {
        Ok(response) => response,
        Err(error) if error.is_connect() || error.is_timeout() => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if !response.status().is_success() {
        return Ok(None);
    }
    let value = response.json::<Value>()?;
    if value["product"] != "greenski" {
        bail!("port {} is occupied by another service", config.port);
    }
    Ok(Some(value))
}

fn http_client() -> Result<reqwest::blocking::Client> {
    Ok(reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_millis(500))
        .timeout(Duration::from_secs(30))
        .build()?)
}

#[cfg(target_os = "macos")]
pub fn install() -> Result<()> {
    let executable = xml_escape(&std::env::current_exe()?.to_string_lossy());
    let stdout = xml_escape(&config::stdout_log_path().to_string_lossy());
    let stderr = xml_escape(&config::stderr_log_path().to_string_lossy());
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>Label</key><string>com.looskis.greenski</string>
<key>ProgramArguments</key><array><string>{executable}</string><string>run</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>{stdout}</string>
<key>StandardErrorPath</key><string>{stderr}</string>
</dict></plist>
"#
    );
    let path = config::launch_agent_path();
    fs::create_dir_all(path.parent().context("LaunchAgents path has no parent")?)?;
    fs::write(&path, plist)?;
    let target = format!("gui/{}", unsafe { libc::getuid() });
    let _ = Command::new("launchctl")
        .args(["bootout", &target, &path.to_string_lossy()])
        .status();
    let result = Command::new("launchctl")
        .args(["bootstrap", &target, &path.to_string_lossy()])
        .status()?;
    if !result.success() {
        bail!("launchctl bootstrap failed with {result}");
    }
    println!("installed and loaded {}", path.display());
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn install() -> Result<()> {
    bail!("service installation is currently implemented for macOS LaunchAgents")
}

#[cfg(target_os = "macos")]
pub fn uninstall() -> Result<()> {
    let path = config::launch_agent_path();
    let target = format!("gui/{}", unsafe { libc::getuid() });
    let _ = Command::new("launchctl")
        .args(["bootout", &target, &path.to_string_lossy()])
        .status();
    if path.exists() {
        fs::remove_file(&path)?;
        println!("removed {}", path.display());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
pub fn uninstall() -> Result<()> {
    bail!("service removal is currently implemented for macOS LaunchAgents")
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
