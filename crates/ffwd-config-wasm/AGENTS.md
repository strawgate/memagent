# ffwd-config-wasm

WASM bindings that power the interactive Config Builder on the docs site.

## Purpose

Exposes a small JavaScript API backed by the same `ffwd-config` Rust types used at runtime, so users can generate and validate `ffwd.yaml` configs in the browser without a server.

## Key exports

| Function | Description |
|----------|-------------|
| `get_input_templates()` | JSON array of input template definitions (id, label, snippet, fields) |
| `get_output_templates()` | JSON array of output template definitions |
| `get_use_case_templates()` | JSON array of preset use-case configs |
| `validate_config(yaml)` | Throws a JS error if the YAML fails Rust-side validation |
| `get_input_snippet(id)` | Returns the raw YAML snippet for a single input template |
| `get_output_snippet(id)` | Returns the raw YAML snippet for a single output template |

## Build

```bash
wasm-pack build crates/ffwd-config-wasm --target web --out-dir ../../book/public/wasm/ffwd-config
```

Output lands in `book/public/wasm/ffwd-config/`. The `.js` and `.wasm` files are committed there.

## Key constraints

- **Compile target**: `wasm32-unknown-unknown` only. No `std::fs`, no `tokio`, no `std::thread`.
- **Template source**: input/output templates come from `ffwd_config::docspec`. Keep the WASM exports aligned with that shared registry rather than reintroducing local copies.
- **Validation parity**: only expose formats and output types that pass `Config::validate`. Check `crates/ffwd-config/src/validate.rs` before adding options.
- **No serde-wasm-bindgen**: uses JSON round-trip via `js_sys::JSON::parse`. Static data — panics on serialization failure by design.
