use crate::utils::StringTokens;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchTarget {
    None,
    Label(String),
    Payload {
        path: StringTokens,
        value: Value,
    },
    Both {
        label: String,
        path: StringTokens,
        value: Value,
    },
}

/// Matchers used to filter routes based on labels and nested payload fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RouteMatcher {
    /// Always returns true regardless of the input.
    Always,
    /// Matches when the label exactly equals the provided value (case-insensitive).
    LabelEquals(String),
    /// Matches when the label starts with the provided prefix (case-insensitive).
    LabelPrefix(String),
    /// Matches when the label ends with the provided suffix (case-insensitive).
    LabelSuffix(String),
    /// Matches when the label contains the provided substring (case-insensitive).
    LabelContains(String),
    /// Matches when the payload value at the given JSON path equals the expected string.
    PayloadPathEquals {
        path: StringTokens,
        expected: String,
    },
    /// Matches when the payload has any value at the provided JSON path.
    PayloadPathExists { path: StringTokens },
    /// Matches when the payload boolean at the provided JSON path equals the expected value.
    PayloadPathBoolEquals { path: StringTokens, expected: bool },
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

            RouteMatcher::PayloadPathContains { path, needle } => payload
                .and_then(|x| path.select_json(x))
                .and_then(|val| val.as_str())
                .map(|s| s.contains(needle))
                .unwrap_or(false),
        }
    }

    /// Returns extraction hints: (label_fragment, payload_path).
    /// `label_fragment` is the significant lowercased label piece used by this matcher (if any).
    /// `payload_path` is the JSON path targeted by this matcher (if any).
    pub fn extract_targets(&self) -> MatchTarget {
        let (label_fragment, payload_path) = match self {
            RouteMatcher::Always => (None, None),

            // label-driven
            RouteMatcher::LabelEquals(s)
            | RouteMatcher::LabelPrefix(s)
            | RouteMatcher::LabelSuffix(s)
            | RouteMatcher::LabelContains(s) => (Some(s.clone()), None),

            // payload-driven
            _ => (None, self.insertion_hint()),
        };

        match (payload_path, label_fragment) {
            (None, None) => MatchTarget::None,
            (None, Some(fragment)) => MatchTarget::Label(fragment),
            (Some(path), None) => MatchTarget::Payload {
                path: path.0.clone(),
                value: path.1,
            },
            (Some(path), Some(fragment)) => MatchTarget::Both {
                label: fragment,
                path: path.0.clone(),
                value: path.1,
            },
        }
    }

    /// Returns a JSON insertion hint that would satisfy this matcher, if applicable.
    /// For payload-driven matchers, returns `(path, value)` to insert so that `matches(...)` becomes `true`.
    /// For label-driven or non-deterministic cases, returns `None`.
    pub fn insertion_hint(&self) -> Option<(&StringTokens, Value)> {
        match self {
            RouteMatcher::PayloadPathEquals { path, expected } => {
                Some((path, Value::String(expected.clone())))
            }
            RouteMatcher::PayloadPathExists { path } => {
                // Любое значение подойдёт; используем Null как нейтральное.
                Some((path, Value::Null))
            }
            RouteMatcher::PayloadPathBoolEquals { path, expected } => {
                Some((path, Value::Bool(*expected)))
            }
            RouteMatcher::PayloadPathContains { path, needle } => {
                // Строка, равная needle, тоже "содержит" needle.
                Some((path, Value::String(needle.clone())))
            }

            // Для label-* матчеров и Always — подсказка для payload отсутствует.
            _ => None,
        }
    }
}
