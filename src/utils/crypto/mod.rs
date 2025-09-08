use anyhow::anyhow;
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

// Кастомная десериализация
impl<'de> Deserialize<'de> for SecretValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum SecretValueDeserialize {
            Simple(String), // Простой случай, когда значение - это просто строка
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

// Кастомная сериализация
impl Serialize for SecretValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Сериализуем только var_name, value затираем пустой строкой
        #[derive(Serialize)]
        struct SecretValueSerialize<'a> {
            value: &'a str,
            var_name: &'a Option<String>,
        }

        let serialized = SecretValueSerialize {
            value: "***", // Затираем значение, чтобы не выводить его в сериализованном виде
            var_name: &self.var_name, // Сохраняем имя переменной окружения, если оно есть
        };

        serialized.serialize(serializer)
    }
}
