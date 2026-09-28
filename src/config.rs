//! `[aws_sqs]` configuration.
//!
//! Sources, in order (later wins):
//! 1. `[aws_sqs]` in `autumn.toml`.
//! 2. `[profile.<name>.aws_sqs]` in `autumn.toml`.
//! 3. `[aws_sqs]` in the profile file, for example `autumn-prod.toml`.
//! 4. `.env` values, then the process environment: `AUTUMN_AWS_SQS__<PATH>`,
//!    for example `AUTUMN_AWS_SQS__QUEUES__DEFAULT=https://...`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::SqsError;
use crate::policy::MAX_VISIBILITY_SECS;

/// Name of the TOML section.
pub const SECTION: &str = "aws_sqs";
/// Metrics label for a queue URL with no alias.
pub const UNCONFIGURED_LABEL: &str = "unconfigured";
/// Prefix for environment overrides.
pub const ENV_PREFIX: &str = "AUTUMN_AWS_SQS__";

/// Plugin configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct SqsConfig {
    /// AWS region. Unset: use the AWS default chain.
    pub region: Option<String>,
    /// Custom endpoint, for example `LocalStack`.
    pub endpoint: Option<String>,
    /// Name of the env var that holds the access key ID.
    pub access_key_id_env: Option<String>,
    /// Name of the env var that holds the secret access key.
    pub secret_access_key_env: Option<String>,
    /// Queue aliases to queue URLs. A `#[job(queue = "x")]` uses alias `x`.
    pub queues: BTreeMap<String, String>,
    /// Job transport settings.
    pub jobs: JobsConfig,
    /// Worker settings.
    pub worker: WorkerConfig,
}

/// Job transport settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct JobsConfig {
    /// Queue alias or URL for jobs whose queue has no alias.
    pub default_queue: String,
    /// Queue alias or URL for dead letters. Unset: use the SQS redrive policy.
    pub dead_letter_queue: Option<String>,
}

impl Default for JobsConfig {
    fn default() -> Self {
        Self {
            default_queue: "default".to_owned(),
            dead_letter_queue: None,
        }
    }
}

/// Worker settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct WorkerConfig {
    /// Long-poll wait, 0 to 20 s.
    pub wait_time_secs: u64,
    /// Messages per receive, 1 to 10.
    pub max_messages: u32,
    /// Visibility timeout, 1 to 43 200 s.
    pub visibility_timeout_secs: u64,
    /// Handlers that run at the same time, per queue.
    pub max_in_flight: usize,
    /// Extend visibility while a handler runs.
    pub heartbeat: bool,
    /// Largest retry backoff, 0 to 43 200 s.
    pub max_backoff_secs: u64,
    /// Time to wait for in-flight handlers at shutdown.
    pub drain_timeout_secs: u64,
    /// Interval to read queue counters for metrics. 0 turns it off.
    pub stats_interval_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            wait_time_secs: 20,
            max_messages: 10,
            visibility_timeout_secs: 30,
            max_in_flight: 16,
            heartbeat: true,
            max_backoff_secs: 900,
            drain_timeout_secs: 20,
            stats_interval_secs: 30,
        }
    }
}

impl SqsConfig {
    /// Reads `[aws_sqs]` from the text of an `autumn.toml` file.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] for bad TOML or unknown keys.
    pub fn from_toml_str(autumn_toml: &str) -> Result<Self, SqsError> {
        Self::from_sources(Some(autumn_toml), None, std::iter::empty())
    }

    /// Merges the base file, the profile file, and env vars. Then validates.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] for bad TOML, unknown keys, or failed
    /// validation.
    pub fn from_sources(
        base: Option<&str>,
        profile: Option<&str>,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, SqsError> {
        Self::from_layers(base, &[], profile, env)
    }

    /// Merges, in order: `[aws_sqs]` in `base`, `[profile.<name>.aws_sqs]` in
    /// `base` for each of `profile_names`, `[aws_sqs]` in `profile_file`, and
    /// env vars. Then validates.
    fn from_layers(
        base: Option<&str>,
        profile_names: &[String],
        profile_file: Option<&str>,
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, SqsError> {
        let parse = |text: &str| -> Result<toml::Table, SqsError> {
            toml::from_str(text).map_err(|e| SqsError::Config(format!("toml: {e}")))
        };
        let mut table = toml::Table::new();
        if let Some(text) = base {
            let file = parse(text)?;
            if let Some(toml::Value::Table(section)) = file.get(SECTION) {
                merge(&mut table, section.clone());
            }
            for name in profile_names {
                if let Some(toml::Value::Table(section)) = file
                    .get("profile")
                    .and_then(|p| p.get(name))
                    .and_then(|p| p.get(SECTION))
                {
                    merge(&mut table, section.clone());
                }
            }
        }
        if let Some(text) = profile_file
            && let Some(toml::Value::Table(section)) = parse(text)?.get(SECTION)
        {
            merge(&mut table, section.clone());
        }
        let schema =
            toml::Table::try_from(Self::default()).map_err(|e| SqsError::Config(e.to_string()))?;
        for (key, value) in env {
            let Some(path) = key.strip_prefix(ENV_PREFIX) else {
                continue;
            };
            let path: Vec<String> = path.split("__").map(str::to_ascii_lowercase).collect();
            let typed = typed_env_value(&schema, &path, &value)
                .ok_or_else(|| SqsError::Config(format!("{key}: value is not valid")))?;
            insert_path(&mut table, &path, typed);
        }
        let cfg: Self = toml::Value::Table(table)
            .try_into()
            .map_err(|e| SqsError::Config(e.to_string()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Loads config from files and from `env`.
    ///
    /// For each file, it reads `manifest_dir/<file>` when that file exists,
    /// else `./<file>`. It reads `autumn.toml`, the inline profile sections,
    /// and the first `autumn-<name>.toml` that exists for `profile_names`.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] for a file that cannot be read or parsed.
    pub fn load_from_dir(
        manifest_dir: &Path,
        profile_names: &[String],
        env: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, SqsError> {
        let find = |file: &str| {
            let candidate = manifest_dir.join(file);
            if candidate.exists() {
                candidate
            } else {
                PathBuf::from(file)
            }
        };
        let base = read_optional(&find("autumn.toml"))?;
        let mut profile = None;
        for name in profile_names {
            if let Some(text) = read_optional(&find(&format!("autumn-{name}.toml")))? {
                profile = Some(text);
                break;
            }
        }
        let inline = inline_profile_order(profile_names);
        Self::from_layers(base.as_deref(), &inline, profile.as_deref(), env)
    }

    /// Loads config like autumn-web does.
    ///
    /// - Files: `$AUTUMN_MANIFEST_DIR` (or the crate dir that
    ///   `#[autumn_web::main]` records), then the current directory.
    /// - Profile: `profile` (the app profile). The raw `AUTUMN_ENV` or
    ///   `AUTUMN_PROFILE` value picks the file spelling, like autumn-web.
    /// - Env: `.env` values, then the process environment.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] when loading or validation fails.
    pub fn load(profile: Option<&str>) -> Result<Self, SqsError> {
        use autumn_web::config::Env as _;
        let os = autumn_web::config::OsEnv;
        let names = profile
            .map(|p| {
                // Same selector order as autumn-web: env vars, then `--profile`.
                let selector = ["AUTUMN_ENV", "AUTUMN_PROFILE"]
                    .iter()
                    .find_map(|k| os.var(k).ok().filter(|v| !v.trim().is_empty()))
                    .or_else(profile_flag)
                    .map_or_else(|| p.to_owned(), |v| v.trim().to_owned());
                autumn_web::config::profile_override_file_lookup_names(p, &selector)
            })
            .unwrap_or_default();
        let dir = os
            .var("AUTUMN_MANIFEST_DIR")
            .map_or_else(|_| PathBuf::from("."), PathBuf::from);
        Self::load_from_dir(&dir, &names, process_env()?)
    }

    /// Checks the values.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] with the first problem found.
    pub fn validate(&self) -> Result<(), SqsError> {
        let bad = |m: String| Err(SqsError::Config(m));
        if self.access_key_id_env.is_some() != self.secret_access_key_env.is_some() {
            return bad(
                "set both access_key_id_env and secret_access_key_env, or neither".to_owned(),
            );
        }
        if let Some(endpoint) = &self.endpoint
            && !is_url(endpoint)
        {
            return bad(format!("endpoint is not an http(s) URL: {endpoint}"));
        }
        for (alias, url) in &self.queues {
            if alias.is_empty() || !is_url(url) {
                return bad(format!("queues.{alias} is not an http(s) URL: {url}"));
            }
        }
        if let Some(dlq) = &self.jobs.dead_letter_queue
            && self.resolve_queue(dlq).is_err()
        {
            return bad(format!("jobs.dead_letter_queue has no queue: {dlq}"));
        }
        let w = &self.worker;
        if !(1..=10).contains(&w.max_messages) {
            return bad(format!(
                "worker.max_messages must be 1 to 10, not {}",
                w.max_messages
            ));
        }
        if w.wait_time_secs > 20 {
            return bad(format!(
                "worker.wait_time_secs must be 0 to 20, not {}",
                w.wait_time_secs
            ));
        }
        if !(1..=MAX_VISIBILITY_SECS).contains(&w.visibility_timeout_secs) {
            return bad(format!(
                "worker.visibility_timeout_secs must be 1 to {MAX_VISIBILITY_SECS}, not {}",
                w.visibility_timeout_secs
            ));
        }
        if w.max_in_flight == 0 {
            return bad("worker.max_in_flight must be 1 or more".to_owned());
        }
        if w.max_backoff_secs > MAX_VISIBILITY_SECS {
            return bad(format!(
                "worker.max_backoff_secs must be 0 to {MAX_VISIBILITY_SECS}, not {}",
                w.max_backoff_secs
            ));
        }
        Ok(())
    }

    /// Returns the URL for a queue alias. A URL input returns unchanged.
    ///
    /// # Errors
    /// Returns [`SqsError::UnknownQueue`] when no alias matches.
    pub fn resolve_queue(&self, alias_or_url: &str) -> Result<String, SqsError> {
        if is_url(alias_or_url) {
            return Ok(alias_or_url.to_owned());
        }
        self.queues
            .get(alias_or_url)
            .cloned()
            .ok_or_else(|| SqsError::UnknownQueue(alias_or_url.to_owned()))
    }

    /// Returns the metrics label for a queue: its alias, or `"unconfigured"`.
    ///
    /// A raw URL never becomes a label. This keeps the label set small and
    /// keeps account IDs out of `/actuator/prometheus`.
    #[must_use]
    pub fn label_for(&self, alias_or_url: &str) -> String {
        if self.queues.contains_key(alias_or_url) {
            return alias_or_url.to_owned();
        }
        self.queues
            .iter()
            .find(|(_, url)| url.as_str() == alias_or_url)
            .map_or_else(|| UNCONFIGURED_LABEL.to_owned(), |(alias, _)| alias.clone())
    }

    /// Reads the static credentials from the named env vars.
    ///
    /// Returns `None` when no env var names are set: use the AWS default chain.
    ///
    /// # Errors
    /// Returns [`SqsError::Config`] for a partial pair or a missing variable.
    pub fn credentials(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<Option<(String, String)>, SqsError> {
        let (Some(key_var), Some(secret_var)) =
            (&self.access_key_id_env, &self.secret_access_key_env)
        else {
            if self.access_key_id_env.is_some() || self.secret_access_key_env.is_some() {
                return Err(SqsError::Config(
                    "set both access_key_id_env and secret_access_key_env, or neither".to_owned(),
                ));
            }
            return Ok(None);
        };
        // Error text names the variable, never the value.
        let read = |var: &str| {
            lookup(var).ok_or_else(|| SqsError::Config(format!("env var {var} is not set")))
        };
        Ok(Some((read(key_var)?, read(secret_var)?)))
    }
}

/// Returns `.env` values, then the process environment (later wins).
///
/// It skips a variable whose name or value is not UTF-8.
pub(crate) fn process_env() -> Result<Vec<(String, String)>, SqsError> {
    let mut vars = autumn_web::dotenv::resolve_process_dotenv()
        .map_err(|e| SqsError::Config(format!(".env: {e}")))?;
    vars.extend(
        std::env::vars_os()
            .filter_map(|(k, v)| Some((k.into_string().ok()?, v.into_string().ok()?))),
    );
    Ok(vars)
}

/// The value of `--profile <name>` or `--profile=<name>` in the process args.
fn profile_flag() -> Option<String> {
    let args: Vec<String> = std::env::args_os()
        .filter_map(|a| a.into_string().ok())
        .collect();
    args.iter().enumerate().find_map(|(i, a)| {
        a.strip_prefix("--profile=").map(str::to_owned).or_else(|| {
            (a == "--profile")
                .then(|| args.get(i + 1).cloned())
                .flatten()
        })
    })
}

/// Inline `[profile.<name>]` merge order of autumn-web: the long alias first,
/// so the short name wins.
fn inline_profile_order(profile_names: &[String]) -> Vec<String> {
    let mut names = profile_names.to_vec();
    names.sort_by_key(|n| match n.as_str() {
        "production" | "development" => 0,
        _ => 1,
    });
    names
}

/// Reads one variable from the process environment, else from `.env`.
pub(crate) fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok().or_else(|| {
        autumn_web::dotenv::resolve_process_dotenv()
            .ok()?
            .into_iter()
            .find_map(|(k, v)| (k == name).then_some(v))
    })
}

/// Returns `true` for an `http://` or `https://` URL.
pub(crate) fn is_url(value: &str) -> bool {
    value.starts_with("https://") || value.starts_with("http://")
}

fn read_optional(path: &Path) -> Result<Option<String>, SqsError> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(SqsError::Config(format!("{}: {e}", path.display()))),
    }
}

/// Deep merge: tables merge by key; other values replace.
fn merge(into: &mut toml::Table, from: toml::Table) {
    for (key, value) in from {
        match (into.get_mut(&key), value) {
            (Some(toml::Value::Table(dst)), toml::Value::Table(src)) => merge(dst, src),
            (_, value) => {
                into.insert(key, value);
            }
        }
    }
}

fn insert_path(table: &mut toml::Table, path: &[String], value: toml::Value) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut cur = table;
    for key in parents {
        let entry = cur
            .entry(key.clone())
            .or_insert_with(|| toml::Value::Table(toml::Table::new()));
        if !entry.is_table() {
            *entry = toml::Value::Table(toml::Table::new());
        }
        let toml::Value::Table(next) = entry else {
            return;
        };
        cur = next;
    }
    cur.insert(last.clone(), value);
}

/// Types an env value like the default value at the same path.
fn typed_env_value(schema: &toml::Table, path: &[String], raw: &str) -> Option<toml::Value> {
    let mut node: Option<&toml::Value> = None;
    let mut table = Some(schema);
    for key in path {
        node = table.and_then(|t| t.get(key));
        table = node.and_then(toml::Value::as_table);
    }
    match node {
        Some(toml::Value::Integer(_)) => raw.parse().ok().map(toml::Value::Integer),
        Some(toml::Value::Boolean(_)) => raw.parse().ok().map(toml::Value::Boolean),
        _ => Some(toml::Value::String(raw.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const URL: &str = "https://sqs.us-east-1.amazonaws.com/123/jobs";

    fn env(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn missing_section_gives_defaults() {
        let cfg = SqsConfig::from_toml_str("[server]\nport = 3000\n").unwrap();
        assert_eq!(cfg, SqsConfig::default());
        assert_eq!(cfg.worker.wait_time_secs, 20);
        assert_eq!(cfg.jobs.default_queue, "default");
    }

    #[test]
    fn reads_section() {
        let toml = format!(
            "[aws_sqs]\nregion = \"us-east-1\"\n[aws_sqs.queues]\ndefault = \"{URL}\"\n\
             [aws_sqs.worker]\nmax_in_flight = 4\n[aws_sqs.jobs]\ndead_letter_queue = \"{URL}-dlq\"\n"
        );
        let cfg = SqsConfig::from_toml_str(&toml).unwrap();
        assert_eq!(cfg.region.as_deref(), Some("us-east-1"));
        assert_eq!(cfg.queues["default"], URL);
        assert_eq!(cfg.worker.max_in_flight, 4);
        assert_eq!(cfg.jobs.dead_letter_queue, Some(format!("{URL}-dlq")));
    }

    #[test]
    fn unknown_key_is_rejected() {
        let err = SqsConfig::from_toml_str("[aws_sqs]\nregoin = \"x\"\n").unwrap_err();
        assert!(
            matches!(err, SqsError::Config(ref m) if m.contains("regoin")),
            "{err:?}"
        );
    }

    #[test]
    fn bad_toml_is_rejected() {
        assert!(matches!(
            SqsConfig::from_toml_str("[aws_sqs"),
            Err(SqsError::Config(_))
        ));
    }

    #[test]
    fn profile_file_overrides_base() {
        let base =
            format!("[aws_sqs]\nregion = \"us-east-1\"\n[aws_sqs.queues]\ndefault = \"{URL}\"\n");
        let prof = "[aws_sqs]\nregion = \"eu-west-1\"\n[aws_sqs.worker]\nmax_messages = 5\n";
        let cfg = SqsConfig::from_sources(Some(&base), Some(prof), env(&[])).unwrap();
        assert_eq!(cfg.region.as_deref(), Some("eu-west-1"));
        assert_eq!(cfg.queues["default"], URL);
        assert_eq!(cfg.worker.max_messages, 5);
    }

    #[test]
    fn env_overrides_files() {
        let base = "[aws_sqs]\nregion = \"us-east-1\"\n";
        let cfg = SqsConfig::from_sources(
            Some(base),
            None,
            env(&[
                ("AUTUMN_AWS_SQS__REGION", "ap-south-1"),
                ("AUTUMN_AWS_SQS__QUEUES__CRITICAL", URL),
                ("AUTUMN_AWS_SQS__WORKER__MAX_IN_FLIGHT", "2"),
                ("AUTUMN_AWS_SQS__WORKER__HEARTBEAT", "false"),
                ("UNRELATED", "x"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.region.as_deref(), Some("ap-south-1"));
        assert_eq!(cfg.queues["critical"], URL);
        assert_eq!(cfg.worker.max_in_flight, 2);
        assert!(!cfg.worker.heartbeat);
    }

    #[test]
    fn env_string_that_looks_numeric_stays_string_for_region() {
        let cfg =
            SqsConfig::from_sources(None, None, env(&[("AUTUMN_AWS_SQS__REGION", "123")])).unwrap();
        assert_eq!(cfg.region.as_deref(), Some("123"));
    }

    #[test]
    fn env_bad_number_is_config_error() {
        let err = SqsConfig::from_sources(
            None,
            None,
            env(&[("AUTUMN_AWS_SQS__WORKER__MAX_IN_FLIGHT", "many")]),
        )
        .unwrap_err();
        assert!(matches!(err, SqsError::Config(_)));
    }

    #[test]
    fn validate_rejects_partial_credentials() {
        let cfg = SqsConfig {
            access_key_id_env: Some("K".into()),
            ..SqsConfig::default()
        };
        assert!(matches!(cfg.validate(), Err(SqsError::Config(ref m)) if m.contains("both")));
    }

    #[test]
    fn validate_rejects_non_url_queue() {
        let mut cfg = SqsConfig::default();
        cfg.queues.insert("default".into(), "jobs".into());
        assert!(matches!(cfg.validate(), Err(SqsError::Config(ref m)) if m.contains("default")));
    }

    #[test]
    fn validate_rejects_worker_ranges() {
        let bad = [
            WorkerConfig {
                max_messages: 0,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                max_messages: 11,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                wait_time_secs: 21,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                visibility_timeout_secs: 0,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                visibility_timeout_secs: MAX_VISIBILITY_SECS + 1,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                max_in_flight: 0,
                ..WorkerConfig::default()
            },
            WorkerConfig {
                max_backoff_secs: MAX_VISIBILITY_SECS + 1,
                ..WorkerConfig::default()
            },
        ];
        for worker in bad {
            let cfg = SqsConfig {
                worker: worker.clone(),
                ..SqsConfig::default()
            };
            assert!(cfg.validate().is_err(), "{worker:?}");
        }
        assert!(SqsConfig::default().validate().is_ok());
    }

    #[test]
    fn validate_rejects_unknown_dead_letter_alias() {
        let cfg = SqsConfig {
            jobs: JobsConfig {
                dead_letter_queue: Some("dlq".into()),
                ..JobsConfig::default()
            },
            ..SqsConfig::default()
        };
        assert!(matches!(cfg.validate(), Err(SqsError::Config(ref m)) if m.contains("dlq")));
    }

    #[test]
    fn resolve_queue_alias_and_url() {
        let mut cfg = SqsConfig::default();
        cfg.queues.insert("default".into(), URL.into());
        assert_eq!(cfg.resolve_queue("default").unwrap(), URL);
        assert_eq!(
            cfg.resolve_queue("http://localhost:4566/0/q").unwrap(),
            "http://localhost:4566/0/q"
        );
        assert_eq!(
            cfg.resolve_queue("nope"),
            Err(SqsError::UnknownQueue("nope".into()))
        );
    }

    #[test]
    fn credentials_from_named_vars() {
        let cfg = SqsConfig {
            access_key_id_env: Some("MY_KEY".into()),
            secret_access_key_env: Some("MY_SECRET".into()),
            ..SqsConfig::default()
        };
        let got = cfg
            .credentials(|k| match k {
                "MY_KEY" => Some("AKIA".into()),
                "MY_SECRET" => Some("s3cr3t".into()),
                _ => None,
            })
            .unwrap();
        assert_eq!(got, Some(("AKIA".into(), "s3cr3t".into())));
        let missing = cfg.credentials(|_| None).unwrap_err();
        assert!(matches!(missing, SqsError::Config(ref m) if m.contains("MY_KEY")));
        assert_eq!(SqsConfig::default().credentials(|_| None).unwrap(), None);
    }

    #[test]
    fn credentials_error_never_contains_secret() {
        let cfg = SqsConfig {
            access_key_id_env: Some("MY_KEY".into()),
            secret_access_key_env: Some("MY_SECRET".into()),
            ..SqsConfig::default()
        };
        let err = cfg
            .credentials(|k| (k == "MY_KEY").then(|| "AKIA-VISIBLE".to_owned()))
            .unwrap_err();
        assert!(!err.to_string().contains("AKIA-VISIBLE"));
    }

    #[test]
    fn inline_profile_section_is_merged() {
        let base = format!(
            "[aws_sqs]\nregion = \"us-east-1\"\n[aws_sqs.queues]\ndefault = \"{URL}\"\n\
             [profile.prod.aws_sqs]\nregion = \"eu-west-1\"\n"
        );
        let cfg =
            SqsConfig::from_layers(Some(&base), &["prod".to_owned()], None, env(&[])).unwrap();
        assert_eq!(cfg.region.as_deref(), Some("eu-west-1"));
        let dev = SqsConfig::from_layers(Some(&base), &["dev".to_owned()], None, env(&[])).unwrap();
        assert_eq!(dev.region.as_deref(), Some("us-east-1"));
    }

    #[test]
    fn inline_order_lets_short_name_win() {
        let names = vec!["prod".to_owned(), "production".to_owned()];
        assert_eq!(inline_profile_order(&names), vec!["production", "prod"]);
        let base = format!(
            "[aws_sqs.queues]\ndefault = \"{URL}\"\n\
             [profile.production.aws_sqs]\nregion = \"long\"\n\
             [profile.prod.aws_sqs]\nregion = \"short\"\n"
        );
        let dir = std::env::temp_dir().join(format!("aws-sqs-inline-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("autumn.toml"), base).unwrap();
        let cfg = SqsConfig::load_from_dir(&dir, &names, env(&[])).unwrap();
        assert_eq!(cfg.region.as_deref(), Some("short"));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn label_for_uses_alias_only() {
        let mut cfg = SqsConfig::default();
        cfg.queues.insert("jobs".into(), URL.into());
        assert_eq!(cfg.label_for("jobs"), "jobs");
        assert_eq!(cfg.label_for(URL), "jobs");
        assert_eq!(cfg.label_for("https://other"), UNCONFIGURED_LABEL);
    }

    #[test]
    fn load_from_dir_reads_files() {
        let dir = std::env::temp_dir().join(format!("aws-sqs-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("autumn.toml"),
            format!("[aws_sqs.queues]\ndefault = \"{URL}\"\n"),
        )
        .unwrap();
        std::fs::write(
            dir.join("autumn-prod.toml"),
            "[aws_sqs]\nregion = \"eu-west-1\"\n",
        )
        .unwrap();
        let names = vec!["production".to_owned(), "prod".to_owned()];
        let cfg = SqsConfig::load_from_dir(&dir, &names, env(&[])).unwrap();
        assert_eq!(cfg.region.as_deref(), Some("eu-west-1"));
        assert_eq!(cfg.queues["default"], URL);
        let none = SqsConfig::load_from_dir(&dir.join("missing"), &[], env(&[])).unwrap();
        assert_eq!(none, SqsConfig::default());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
