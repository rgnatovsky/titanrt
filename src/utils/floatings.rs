/// Utility functions for formatting and parsing numbers.

/// Parses a string slice into an `Option<f64>`.
pub fn parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

/// Parses an `&Option<String>` into an `Option<f64>`.
pub fn parse_f64_opt(s: &Option<String>) -> Option<f64> {
    s.as_deref().and_then(|x| x.parse::<f64>().ok())
}

/// Rounds a floating-point number to the specified number of decimal places.
pub fn round_f64(value: f64, precision: i32) -> f64 {
    let factor = 10f64.powi(precision);
    (value * factor).round() / factor
}

/// Rounds an `Option<f64>` to the specified number of decimal places.
pub fn round_f64_opt(value: Option<f64>, precision: i32) -> Option<f64> {
    value.map(|v| round_f64(v, precision))
}

/// Determines the number of decimal places in a string representation of a number,
/// using the specified delimiter (e.g., '.' for decimal point).    
///
/// # Examples
/// ```
/// let decimals = decimals_from_string("0.12345", '.');
/// assert_eq!(decimals, 5);
/// ```
pub fn decimals_from_string(s: &str, delimiter: char) -> u32 {
    s.split(delimiter)
        .nth(1)
        .map(|frac| frac.trim_end_matches('0').len() as u32)
        .unwrap_or(0)
}

/// Determines the number of decimal places in a floating-point number.
/// This function formats the float to a string with up to 12 decimal places,
/// then counts the number of significant digits after the decimal point.
///
/// # Examples
/// ```
/// let decimals = decimals_from_float(0.1234500);
/// assert_eq!(decimals, 6);
/// ```
pub fn decimals_from_float(step: f64) -> u32 {
    if step <= 0.0 {
        return 0;
    }

    let s = format!("{:.12}", step);
    if let Some(frac) = s.split('.').nth(1) {
        frac.trim_end_matches('0').len() as u32
    } else {
        0
    }
}

/// Calculates the step size corresponding to a given precision (number of decimal places).
pub fn step_from_precision(precision: u32) -> f64 {
    10f64.powi(-(precision as i32))
}
