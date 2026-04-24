#!/bin/bash
cat crates/logfwd-io/tests/it/otlp_receiver_contract.rs | head -n 150 | grep -n -B 5 -A 20 "poll receiver"
