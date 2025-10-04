use crate::utils::StringTokens;
use serde_json::Value;
use std::collections::HashSet;

/// Matchers used to filter routes based on labels and nested payload fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteMatcher {
    /// Always returns true regardless of the input.
    Always,
    /// Matches when the label exactly equals the provided value (case-insensitive).
    LabelEquals(String),
    /// Matches when the label starts with the provided prefix (case-insensitive).
    LabelPrefix(String),
    /// Matches when the payload value at the given JSON path equals the expected string.
    PayloadPathEquals {
        path: StringTokens,
        expected: String,
    },

    /// Matches when the label ends with the provided suffix (case-insensitive).
    LabelSuffix(String),
    /// Matches when the label contains the provided substring (case-insensitive).
    LabelContains(String),

    /// Matches when the payload has any value at the provided JSON path.
    PayloadPathExists { path: StringTokens },
    /// Matches when the payload boolean at the provided JSON path equals the expected value.
    PayloadPathBoolEquals { path: StringTokens, expected: bool },
    /// Matches when the payload string at the provided JSON path is a member of the provided set.
    PayloadPathIn {
        path: StringTokens,
        options: HashSet<String>,
    },
    /// Matches when the payload string at the provided JSON path contains the provided substring.
    PayloadPathContains { path: StringTokens, needle: String },
}

impl RouteMatcher {
    /// Creates a matcher that always returns true.
    pub fn always() -> Self {
        RouteMatcher::Always
    }

    /// Creates a matcher that succeeds when the label exactly equals the provided value.
    pub fn label_equals(value: impl Into<String>) -> Self {
        RouteMatcher::LabelEquals(value.into().to_ascii_lowercase())
    }

    /// Creates a matcher that succeeds when the label starts with the provided prefix.
    pub fn label_prefix(prefix: impl Into<String>) -> Self {
        RouteMatcher::LabelPrefix(prefix.into().to_ascii_lowercase())
    }

    /// Creates a matcher that succeeds when the payload string at the path equals the expected value.
    pub fn payload_path_equals(path: impl AsRef<str>, expected: impl Into<String>) -> Self {
        RouteMatcher::PayloadPathEquals {
            path: StringTokens::parse(path.as_ref()),
            expected: expected.into(),
        }
    }

    /// Creates a matcher that succeeds when the label ends with the provided suffix.
    pub fn label_suffix(suffix: impl Into<String>) -> Self {
        RouteMatcher::LabelSuffix(suffix.into().to_ascii_lowercase())
    }

    /// Creates a matcher that succeeds when the label contains the provided substring.
    pub fn label_contains(substr: impl Into<String>) -> Self {
        RouteMatcher::LabelContains(substr.into().to_ascii_lowercase())
    }

    /// Creates a matcher that succeeds when the payload contains any value at the provided path.
    pub fn payload_path_exists(path: impl AsRef<str>) -> Self {
        RouteMatcher::PayloadPathExists {
            path: StringTokens::parse(path.as_ref()),
        }
    }

    /// Creates a matcher that succeeds when the payload boolean at the path equals the expected value.
    pub fn payload_path_bool_equals(path: impl AsRef<str>, expected: bool) -> Self {
        RouteMatcher::PayloadPathBoolEquals {
            path: StringTokens::parse(path.as_ref()),
            expected,
        }
    }

    /// Creates a matcher that succeeds when the payload string at the path is present in the provided set.
    pub fn payload_path_in(
        path: impl AsRef<str>,
        options: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let set = options.into_iter().map(|s| s.into()).collect();
        RouteMatcher::PayloadPathIn {
            path: StringTokens::parse(path.as_ref()),
            options: set,
        }
    }

    /// Creates a matcher that succeeds when the payload string at the path contains the provided substring.
    pub fn payload_path_contains(path: impl AsRef<str>, needle: impl Into<String>) -> Self {
        RouteMatcher::PayloadPathContains {
            path: StringTokens::parse(path.as_ref()),
            needle: needle.into(),
        }
    }

    /// Returns true when the matcher conditions are satisfied for the provided label and payload.
    pub fn matches(&self, label: Option<&str>, payload: Option<&Value>) -> bool {
        match self {
            RouteMatcher::Always => true,
            RouteMatcher::LabelEquals(expected) => label
                .map(|v| v.to_ascii_lowercase())
                .map(|v| v == *expected)
                .unwrap_or(false),
            RouteMatcher::LabelPrefix(prefix) => label
                .map(|v| v.to_ascii_lowercase())
                .map(|v| v.starts_with(prefix))
                .unwrap_or(false),
            RouteMatcher::PayloadPathEquals { path, expected } => payload
                .and_then(|x| path.select_json(x))
                .and_then(|val| val.as_str())
                .map(|found| found == expected)
                .unwrap_or(false),

            RouteMatcher::LabelSuffix(suffix) => label
                .map(|v| v.to_ascii_lowercase())
                .map(|v| v.ends_with(suffix))
                .unwrap_or(false),
            RouteMatcher::LabelContains(substr) => label
                .map(|v| v.to_ascii_lowercase())
                .map(|v| v.contains(substr))
                .unwrap_or(false),

            RouteMatcher::PayloadPathExists { path } => {
                payload.and_then(|x| path.select_json(x)).is_some()
            }
            RouteMatcher::PayloadPathBoolEquals { path, expected } => payload
                .and_then(|x| path.select_json(x))
                .and_then(|v| v.as_bool())
                .map(|b| b == *expected)
                .unwrap_or(false),
            RouteMatcher::PayloadPathIn { path, options } => payload
                .and_then(|x| path.select_json(x))
                .and_then(|val| val.as_str())
                .map(|s| options.contains(s))
                .unwrap_or(false),
            RouteMatcher::PayloadPathContains { path, needle } => payload
                .and_then(|x| path.select_json(x))
                .and_then(|val| val.as_str())
                .map(|s| s.contains(needle))
                .unwrap_or(false),
        }
    }
}
