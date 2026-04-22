sed -i 's/let transform_count = if spec.config.transform.is_some() {/let transform_count = usize::from(spec.config.transform.is_some());/g' crates/logfwd-runtime/src/pipeline/topology.rs
sed -i '/1/d' crates/logfwd-runtime/src/pipeline/topology.rs
sed -i '/} else {/d' crates/logfwd-runtime/src/pipeline/topology.rs
sed -i '/0/d' crates/logfwd-runtime/src/pipeline/topology.rs
sed -i '/};/d' crates/logfwd-runtime/src/pipeline/topology.rs
