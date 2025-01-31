use regex::Regex;

pub fn sql_parser(stm: &str) -> Vec<&str> {
    let pattern: &str = r#""(s3://[^"]+|file://[^"]+)""#;

    let re = Regex::new(pattern).unwrap();
    re.captures_iter(stm)
        .filter_map(|cap| cap.get(1).map(|m| m.as_str()))
        .collect()
}