//! A command line as the tools read it: `--key value` pairs and bare
//! `--flags`, in any order. The first occurrence of a key counts. Options a
//! tool does not know are left alone, so scripts can pass one command line
//! to several tools.

use std::str::FromStr;

/// The command line, its program name left out.
#[derive(Clone, Debug, Default)]
pub struct Args(Vec<String>);

impl Args {
    pub fn new(args: Vec<String>) -> Self {
        Self(args)
    }

    /// The process's own command line.
    pub fn from_env() -> Self {
        Self(std::env::args().skip(1).collect())
    }

    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    /// The value after `key`.
    pub fn value(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .position(|a| a == key)
            .and_then(|i| self.0.get(i + 1))
            .map(String::as_str)
    }

    /// Every value after `key`, for an option given more than once.
    pub fn values(&self, key: &str) -> Vec<&str> {
        self.0
            .windows(2)
            .filter(|w| w[0] == key)
            .map(|w| w[1].as_str())
            .collect()
    }

    /// Whether the bare `key` is there.
    pub fn flag(&self, key: &str) -> bool {
        self.0.iter().any(|a| a == key)
    }

    /// The value after `key`, parsed, or `default`.
    pub fn number<T: FromStr>(&self, key: &str, default: T) -> Result<T, String> {
        match self.value(key) {
            Some(v) => v.parse().map_err(|_| format!("{key} {v}: not a number it takes")),
            None => Ok(default),
        }
    }

    /// The value after `key`, parsed, if there is one.
    pub fn optional<T: FromStr>(&self, key: &str) -> Result<Option<T>, String> {
        self.value(key)
            .map(|v| v.parse().map_err(|_| format!("{key} {v}: not a number it takes")))
            .transpose()
    }

    /// The value after `key`, which must be there.
    pub fn required(&self, key: &str) -> Result<&str, String> {
        self.value(key).ok_or_else(|| format!("{key} is required"))
    }
}

/// A duration as `N`, `Nms`, `Ns` or `Nm`: milliseconds when bare.
pub fn parse_duration(text: &str) -> Option<std::time::Duration> {
    use std::time::Duration;
    let (digits, unit) = if let Some(d) = text.strip_suffix("ms") {
        (d, 1)
    } else if let Some(d) = text.strip_suffix('s') {
        (d, 1_000)
    } else if let Some(d) = text.strip_suffix('m') {
        (d, 60_000)
    } else {
        (text, 1)
    };
    digits.parse::<u64>().ok()?.checked_mul(unit).map(Duration::from_millis)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn args(line: &str) -> Args {
        Args::new(line.split_whitespace().map(String::from).collect())
    }

    #[test]
    fn reads_values_flags_and_numbers() {
        let a = args("--node 2 --spin --x 1 --x 2 --unknown thing");
        assert_eq!(a.value("--node"), Some("2"));
        assert_eq!(a.values("--x"), ["1", "2"]);
        assert!(a.flag("--spin") && !a.flag("--sync"));
        assert_eq!(a.number("--node", 0usize), Ok(2));
        assert_eq!(a.number("--members", 3usize), Ok(3));
        assert!(a.number::<usize>("--unknown", 0).is_err());
        assert_eq!(a.optional::<i32>("--missing"), Ok(None));
        assert!(a.required("--missing").is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("250"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("250ms"), Some(Duration::from_millis(250)));
        assert_eq!(parse_duration("5s"), Some(Duration::from_secs(5)));
        assert_eq!(parse_duration("2m"), Some(Duration::from_secs(120)));
        assert_eq!(parse_duration("x"), None);
        assert_eq!(parse_duration("-1s"), None);
    }
}
