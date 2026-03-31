//! Fuzz all FormatParser implementations with arbitrary bytes.

#![no_main]
use libfuzzer_sys::fuzz_target;
use logfwd_io::format::{CriParser, FormatParser, JsonParser, RawParser};

fn check_output_size(out: &[u8], input_len: usize, parser_name: &str) {
    assert!(
        out.len() < input_len * 20 + 4096,
        "{parser_name}: output suspiciously large: {} bytes for {input_len} bytes input",
        out.len(),
    );
}

fuzz_target!(|data: &[u8]| {
    let mut out = Vec::with_capacity(data.len() * 2);

    // JsonParser
    let mut json = JsonParser::new();
    out.clear();
    let _ = json.process(data, &mut out);
    check_output_size(&out, data.len(), "JsonParser(1)");
    out.clear();
    let _ = json.process(data, &mut out);
    check_output_size(&out, data.len(), "JsonParser(2)");
    json.reset();
    out.clear();
    let _ = json.process(data, &mut out);
    check_output_size(&out, data.len(), "JsonParser(reset)");

    // RawParser
    let mut raw = RawParser::new();
    out.clear();
    let _ = raw.process(data, &mut out);
    check_output_size(&out, data.len(), "RawParser(1)");
    out.clear();
    let _ = raw.process(data, &mut out);
    check_output_size(&out, data.len(), "RawParser(2)");
    raw.reset();
    out.clear();
    let _ = raw.process(data, &mut out);
    check_output_size(&out, data.len(), "RawParser(reset)");

    // CriParser
    let mut cri = CriParser::new(64 * 1024);
    out.clear();
    let _ = cri.process(data, &mut out);
    check_output_size(&out, data.len(), "CriParser(1)");
    out.clear();
    let _ = cri.process(data, &mut out);
    check_output_size(&out, data.len(), "CriParser(2)");
    cri.reset();
    out.clear();
    let _ = cri.process(data, &mut out);
    check_output_size(&out, data.len(), "CriParser(reset)");
});
