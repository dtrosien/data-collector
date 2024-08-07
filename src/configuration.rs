use secrecy::{ExposeSecret, Secret};
use serde::Deserialize;
use serde_aux::field_attributes::deserialize_number_from_string;
use serde_with::formats::{CommaSeparator, SpaceSeparator};
use serde_with::serde_as;
use serde_with::{DefaultOnError, DisplayFromStr, StringWithSeparator, VecSkipError};
use sqlx::postgres::{PgConnectOptions, PgSslMode};
use tracing::error;

use crate::actions::action::ActionType;
use crate::actions::collector_sources::CollectorSource;
use crate::actions::sp500_fields;

#[derive(Deserialize)]
pub struct Settings {
    pub database: DatabaseSettings,
    pub application: ApplicationSettings,
}

#[derive(Deserialize)]
pub struct DatabaseSettings {
    pub username: String,
    pub password: Secret<String>,
    #[serde(deserialize_with = "deserialize_number_from_string")]
    pub port: u16,
    pub host: String,
    pub database_name: String,
    pub require_ssl: bool,
}

#[serde_as]
#[derive(Deserialize)]
pub struct ApplicationSettings {
    pub task_dependencies: Vec<TaskDependency>,
    pub tasks: Vec<TaskSetting>,
    pub http_client: HttpClientSettings,
    #[serde_as(deserialize_as = "DefaultOnError")]
    #[serde(default)]
    pub secrets: SecretKeys,
}

#[derive(Deserialize, Clone)]
pub struct TaskDependency {
    pub name: TaskName,
    pub dependencies: Vec<TaskName>,
}

pub type TaskName = String;

#[derive(Deserialize, Clone)]
pub struct TaskSetting {
    pub name: TaskName,
    pub comment: Option<String>,
    pub task_type: ActionType,
    #[serde(default = "default_sp500_fields")]
    pub sp500_fields: Vec<sp500_fields::Fields>,
    #[serde(default = "default_include_source")]
    pub include_sources: Vec<CollectorSource>,
    #[serde(default = "default_exclude_source")]
    pub exclude_sources: Vec<CollectorSource>,
}

#[derive(Deserialize, Clone)]
pub struct HttpClientSettings {
    pub timeout_milliseconds: u64,
}

#[serde_as]
#[derive(Deserialize, Clone, Debug)]
pub struct SecretKeys {
    pub polygon: Option<Secret<String>>,
    #[serde_as(deserialize_as = "Option<DefaultOnError>")]
    pub polygon_vec: Option<String>,
    #[serde_as(deserialize_as = "Option<DefaultOnError>")]
    pub financialmodelingprep_company: Option<String>,
}

impl Default for SecretKeys {
    fn default() -> Self {
        error!("Defaulting all secrets to None! Please check all env inputs for keys problems.");
        Self {
            polygon: Default::default(),
            polygon_vec: Default::default(),
            financialmodelingprep_company: Default::default(),
        }
    }
}

impl HttpClientSettings {
    pub fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.timeout_milliseconds)
    }
}

fn default_sp500_fields() -> Vec<sp500_fields::Fields> {
    vec![]
}

fn default_include_source() -> Vec<CollectorSource> {
    vec![CollectorSource::All]
}

fn default_exclude_source() -> Vec<CollectorSource> {
    vec![]
}

impl DatabaseSettings {
    pub fn without_db(&self) -> PgConnectOptions {
        let ssl_mode = if self.require_ssl {
            PgSslMode::Require
        } else {
            // Try an encrypted connection, fallback to unencrypted if it fails
            PgSslMode::Prefer
        };
        PgConnectOptions::new()
            .host(&self.host)
            .username(&self.username)
            .password(self.password.expose_secret())
            .port(self.port)
            .ssl_mode(ssl_mode)
    }

    pub fn with_db(&self) -> PgConnectOptions {
        self.without_db().database(&self.database_name)
    }
}

pub fn get_configuration() -> Result<Settings, config::ConfigError> {
    let base_path = std::env::current_dir().expect("Failed to determine the current directory");
    let configuration_directory = base_path.join("configuration");

    // Detect the running environment.
    // Default to `local` if unspecified.
    let environment: Environment = std::env::var("APP_ENVIRONMENT")
        .unwrap_or_else(|_| "local".into())
        .try_into()
        .expect("Failed to parse APP_ENVIRONMENT.");
    let environment_filename = format!("{}.yaml", environment.as_str());

    // init config reader
    let settings = config::Config::builder()
        // add config values from files
        .add_source(config::File::from(
            configuration_directory.join("base.yaml"),
        ))
        .add_source(config::File::from(
            configuration_directory.join(environment_filename),
        ))
        // Add in settings from environment variables (with a prefix of APP and
        // '__' as separator)
        // E.g. `APP_APPLICATION__PORT=5001 would set `Settings.application.port`
        .add_source(
            config::Environment::with_prefix("APP")
                .prefix_separator("_")
                .separator("__"),
        )
        .build()?;
    // convert to Settings type
    println!("#### {:?}", settings);
    settings.try_deserialize::<Settings>()
}

pub enum Environment {
    Local,
    Production,
}

impl Environment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Environment::Local => "local",
            Environment::Production => "production",
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "production" => Ok(Self::Production),
            other => Err(format!(
                "{} is not a supported environment. \
                Use either `local` or `production`.",
                other
            )),
        }
    }
}
