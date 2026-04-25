pub(super) fn extract_timestamp(line: &str) -> &str {
    let start = line.find("\"timestamp\":\"").unwrap() + 13;
    let end = start + line[start..].find('"').unwrap();
    &line[start..end]
}
