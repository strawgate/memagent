#!/bin/bash
cat crates/logfwd-runtime/src/pipeline/pipeline_tests/failpoints.rs | grep -n -B 5 -A 20 "test_transform_filter_all_rows_does_not_crash"
