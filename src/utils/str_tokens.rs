use ahash::AHashMap;
use serde_json::Value;

/// String tokens
///
///
/// # Example
///
/// ```
/// use titanrt::utils::str_tokens::StringTokens;
///
/// let mut tokens = StringTokens::parse("a.b.c");
/// assert_eq!(tokens.len(), 3);
/// assert_eq!(tokens.by_index(0), Some("a"));
/// assert_eq!(tokens.by_index(1), Some("b"));
/// assert_eq!(tokens.by_index(2), Some("c"));
///
/// tokens.set_alias("first".to_string(), 0);
/// assert_eq!(tokens.by_alias("a"), Some("a"));
///
/// assert_eq!(tokens.by_alias("d"), None);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringTokens {
    segments: Vec<String>,
    aliases: AHashMap<String, usize>,
}

impl StringTokens {
    /// Parse a string into tokens.
    /// Tokens are separated by `separators`
    pub fn parse_custom(raw: &str, separators: &[char]) -> Self {
        let segments = raw
            .split(|c| separators.contains(&c))
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase())
            .collect();
        Self {
            segments,
            aliases: AHashMap::new(),
        }
    }
    /// Parse a string into tokens.
    /// Tokens are separated by `:` `.` `/` `-`
    pub fn parse(raw: &str) -> Self {
        let separators = [':', '.', '/', '-'];
        let segments = raw
            .split(|c| separators.contains(&c))
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase())
            .collect();
        Self {
            segments,
            aliases: AHashMap::new(),
        }
    }
    /// Set an alias for a token
    pub fn set_alias(&mut self, alias: String, index: usize) {
        self.aliases.insert(alias, index);
    }

    /// Get a token by alias
    pub fn by_alias(&self, alias: &str) -> Option<&str> {
        if let Some(index) = self.aliases.get(alias) {
            return self.segments.get(*index).map(|s| s.as_str());
        }
        None
    }

    /// Get a token by index
    pub fn get(&self, index: usize) -> Option<&str> {
        self.segments.get(index).map(|s| s.as_str())
    }

    /// Get all tokens as slice
    pub fn segments(&self) -> &[String] {
        &self.segments
    }

    /// Get all tokens as vector
    pub fn into_segments(self) -> Vec<String> {
        self.segments
    }

    /// Select a value from a JSON object or array by tokens
    /// Returns `None` if the value is not found
    ///
    /// # Example
    ///
    /// ```
    /// use titanrt::utils::str_tokens::StringTokens;
    /// use serde_json::json;
    ///
    /// let tokens = StringTokens::parse("a.b.c");
    /// let value = json!({"a": {"b": {"c": 1}}});
    /// assert_eq!(tokens.select_json(&value), Some(&json!(1)));
    /// ```
    pub fn select_json<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let mut current = value;
        for segment in self.segments.iter() {
            match current {
                Value::Object(map) => {
                    current = map.get(segment)?;
                }
                Value::Array(arr) => {
                    let idx: usize = segment.parse().ok()?;
                    current = arr.get(idx)?;
                }
                _ => return None,
            }
        }
        Some(current)
    }

    /// Inserts (or replaces) a value into a JSON structure at this token path.
    /// Creates intermediate objects/arrays as needed. Returns `true` on success.
    pub fn insert_json(&self, target: &mut Value, new_value: Value) -> bool {
        // Empty path replaces the whole target
        if self.segments.is_empty() {
            *target = new_value;
            return true;
        }

        let mut current = target;

        for (i, segment) in self.segments.iter().enumerate() {
            let is_last = i + 1 == self.segments.len();

            // If segment is a number -> treat as array index
            if let Ok(idx) = segment.parse::<usize>() {
                // Ensure current is an array; if not, replace with empty array
                if !current.is_array() {
                    *current = Value::Array(Vec::new());
                }
                let arr = current.as_array_mut().unwrap();

                // Grow array if needed
                if idx >= arr.len() {
                    arr.resize(idx + 1, Value::Null);
                }

                if is_last {
                    arr[idx] = new_value;
                    return true;
                } else {
                    // Prepare next container if it's currently Null/non-container
                    let next_is_index = self
                        .segments
                        .get(i + 1)
                        .and_then(|s| s.parse::<usize>().ok())
                        .is_some();

                    if arr[idx].is_null() || !(arr[idx].is_object() || arr[idx].is_array()) {
                        arr[idx] = if next_is_index {
                            Value::Array(Vec::new())
                        } else {
                            Value::Object(serde_json::Map::new())
                        };
                    }

                    current = &mut arr[idx];
                }
            } else {
                // Object key
                if !current.is_object() {
                    *current = Value::Object(serde_json::Map::new());
                }
                let map = current.as_object_mut().unwrap();

                if is_last {
                    map.insert(segment.clone(), new_value);
                    return true;
                } else {
                    let next_is_index = self
                        .segments
                        .get(i + 1)
                        .and_then(|s| s.parse::<usize>().ok())
                        .is_some();

                    let entry = map.entry(segment.clone()).or_insert_with(|| {
                        if next_is_index {
                            Value::Array(Vec::new())
                        } else {
                            Value::Object(serde_json::Map::new())
                        }
                    });
                    current = entry;
                }
            }
        }

        true
    }
}
