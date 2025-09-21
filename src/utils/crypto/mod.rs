use anyhow::anyhow;
use dotenvy::{dotenv, from_filename};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::env;

#[derive(Debug, Clone, Default)]
pub struct SecretValue {
    value: String,
    pub var_name: Option<String>,
}

impl SecretValue {
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn set_value(&mut self, value: String) {
        self.value = value;
    }

    pub fn parse_value_from_env(&mut self) -> anyhow::Result<()> {
        if let Some(ref var_name) = self.var_name {
            if let Ok(env_value) = env::var(var_name) {
                self.value = env_value;
                return Ok(());
            }
            return Err(anyhow!("Environment variable {} not found", var_name));
        };

        Err(anyhow!("Environment variable name not set"))
    }
}

impl<'de> Deserialize<'de> for SecretValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum SecretValueDeserialize {
            Simple(String),
            Detailed {
                value: String,
                var_name: Option<String>,
            },
        }

        match SecretValueDeserialize::deserialize(deserializer)? {
            SecretValueDeserialize::Simple(value) => Ok(SecretValue {
                value,
                var_name: None,
            }),
            SecretValueDeserialize::Detailed { value, var_name } => {
                let mut sv = SecretValue { value, var_name };
                match sv.parse_value_from_env() {
                    Ok(_) => {}
                    Err(e) => {
                        println!("{}", e);
                    }
                }

                Ok(sv)
            }
        }
    }
}

impl Serialize for SecretValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct SecretValueSerialize<'a> {
            value: &'a str,
            var_name: &'a Option<String>,
        }

        let serialized = SecretValueSerialize {
            value: "***",
            var_name: &self.var_name,
        };

        serialized.serialize(serializer)
    }
}

pub fn load_dotenv(path: Option<impl AsRef<str>>, preserve: bool) {
    if preserve {
        match path {
            Some(p) => match dotenvy::from_filename(p.as_ref()) {
                Ok(_) => println!("Loaded .env file from {}", p.as_ref()),
                Err(e) => println!("No .env file found at {}: {}", p.as_ref(), e),
            },
            None => match from_filename(".env") {
                Ok(_) => println!("Loaded .env file from root directory"),
                Err(e) => println!("No .env file found at root directory: {}", e),
            },
        }
    } else {
        match path {
            Some(p) => match dotenvy::from_filename_override(p.as_ref()) {
                Ok(_) => println!("Loaded env file from {}", p.as_ref()),
                Err(e) => println!("No env file found at {}: {}", p.as_ref(), e),
            },
            None => match dotenvy::from_filename_override(".env") {
                Ok(_) => println!("Loaded .env file from root directory"),
                Err(e) => println!("No .env file found at root directory: {}", e),
            },
        }
    }
}
