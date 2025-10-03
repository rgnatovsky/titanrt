use std::{borrow::Borrow, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SharedStr(Arc<str>);

impl Serialize for SharedStr {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for SharedStr {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(SharedStr::from(value))
    }
}

impl Default for SharedStr {
    fn default() -> Self {
        Self(Arc::<str>::from("default"))
    }
}

impl SharedStr {
    pub fn new(name: impl AsRef<str>) -> Self {
        Self(Arc::<str>::from(name.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for SharedStr {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for SharedStr {
    fn from(value: String) -> Self {
        Self(Arc::<str>::from(value))
    }
}

impl<'a> From<&'a str> for SharedStr {
    fn from(value: &'a str) -> Self {
        Self(Arc::<str>::from(value))
    }
}

impl From<Arc<str>> for SharedStr {
    fn from(value: Arc<str>) -> Self {
        Self(value)
    }
}

impl From<&SharedStr> for SharedStr {
    fn from(value: &SharedStr) -> Self {
        value.clone()
    }
}

impl AsRef<str> for SharedStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for SharedStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
