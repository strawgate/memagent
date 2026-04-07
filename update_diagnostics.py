import re

with open("crates/logfwd-io/src/diagnostics.rs", "r") as f:
    content = f.read()

# Replace serve_live logic
serve_live_old = """    fn serve_live(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let uptime = self.start_time.elapsed().as_secs();
        let body = format!(
            r#"{"status":"live","uptime_seconds":{},"version":"{}"}"#,
            uptime, VERSION,
        );
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }"""
serve_live_new = """    fn serve_live(&self, request: tiny_http::Request) -> Result<(), Box<dyn std::error::Error>> {
        let uptime = self.start_time.elapsed().as_secs();

        let response_model = crate::diagnostics::models::LiveResponse {
            status: "live".to_string(),
            reason: "process_running".to_string(),
            observed_at_unix_ns: now_nanos().to_string(),
        };
        // Use an ad-hoc struct wrapper to keep existing endpoints shape
        #[derive(serde::Serialize)]
        struct CompatLiveResponse {
            status: String,
            uptime_seconds: u64,
            version: String,
            contract_version: String,
        }
        let compat_response = CompatLiveResponse {
            status: "live".to_string(),
            uptime_seconds: uptime,
            version: VERSION.to_string(),
            contract_version: crate::diagnostics::models::DIAGNOSTICS_CONTRACT_VERSION.to_string(),
        };

        let body = serde_json::to_string(&compat_response)?;
        let header = tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
            .map_err(|()| io::Error::other("invalid HTTP header"))?;
        let resp = tiny_http::Response::from_string(body).with_header(header);
        request.respond(resp)?;
        Ok(())
    }"""
content = content.replace(serve_live_old, serve_live_new)

# Replace serve_ready logic
serve_ready_old = """        if ready {
            let body = format!(
                r#"{"status":"ready","reason":"{}","observed_at_unix_ns":"{}"}"#,
                reason, observed_at_unix_ns
            );
            let resp = tiny_http::Response::from_string(body).with_header(header);
            request.respond(resp)?;
        } else {
            let body = format!(
                r#"{"status":"not_ready","reason":"{}","observed_at_unix_ns":"{}"}"#,
                reason, observed_at_unix_ns
            );
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(503)
                .with_header(header);
            request.respond(resp)?;
        }"""
serve_ready_new = """        let response_model = crate::diagnostics::models::ReadyResponse {
            status: if ready { "ready".to_string() } else { "not_ready".to_string() },
            reason: reason.to_string(),
            observed_at_unix_ns: observed_at_unix_ns.to_string(),
        };
        // Compat struct wrapper
        #[derive(serde::Serialize)]
        struct CompatReadyResponse {
            status: String,
            reason: String,
            observed_at_unix_ns: String,
            contract_version: String,
        }
        let compat_response = CompatReadyResponse {
            status: response_model.status,
            reason: response_model.reason,
            observed_at_unix_ns: response_model.observed_at_unix_ns,
            contract_version: crate::diagnostics::models::DIAGNOSTICS_CONTRACT_VERSION.to_string(),
        };

        let body = serde_json::to_string(&compat_response)?;
        if ready {
            let resp = tiny_http::Response::from_string(body).with_header(header);
            request.respond(resp)?;
        } else {
            let resp = tiny_http::Response::from_string(body)
                .with_status_code(503)
                .with_header(header);
            request.respond(resp)?;
        }"""
content = content.replace(serve_ready_old, serve_ready_new)

# Replace status_body logic
status_body_old = """    fn status_body(&self) -> String {
        let uptime = self.start_time.elapsed().as_secs();
        let observed_at_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        let mut pipelines_json = Vec::new();

        for pm in &self.pipelines {
            let inputs_json: Vec<String> = pm
                .inputs
                .iter()
                .map(|(name, typ, stats)| {
                    format!(
                        r#"{"name":"{}","type":"{}","health":"{}","lines_total":{},"bytes_total":{},"errors":{},"rotations":{},"parse_errors":{}}"#,
                        esc(name),
                        esc(typ),
                        stats.health().as_str(),
                        stats.lines(),
                        stats.bytes(),
                        stats.errors(),
                        stats.rotations(),
                        stats.parse_errors(),
                    )
                })
                .collect();

            let lines_in = pm.transform_in.lines();
            // lines_out: derived from output-sink stats (single increment path —
            // each sink calls inc_lines once on successful delivery). For fan-out
            // pipelines, the maximum across all outputs is used as a proxy for
            // "lines delivered to the most successful output". This may undercount
            // in partial-failure fan-out scenarios where outputs succeed at
            // different rates.
            let lines_out: u64 = pm
                .outputs
                .iter()
                .map(|(_, _, s)| s.lines())
                .max()
                .unwrap_or(0);
            let drop_rate = if lines_in > 0 {
                1.0 - (lines_out as f64 / lines_in as f64)
            } else {
                0.0
            };

            let outputs_json: Vec<String> = pm
                .outputs
                .iter()
                .map(|(name, typ, stats)| {
                    format!(
                        r#"{"name":"{}","type":"{}","health":"{}","lines_total":{},"bytes_total":{},"errors":{}}"#,
                        esc(name),
                        esc(typ),
                        stats.health().as_str(),
                        stats.lines(),
                        stats.bytes(),
                        stats.errors(),
                    )
                })
                .collect();

            let batches = pm.batches_total.load(Ordering::Relaxed);
            let batch_rows = pm.batch_rows_total.load(Ordering::Relaxed);
            let avg_rows = if batches > 0 {
                batch_rows as f64 / batches as f64
            } else {
                0.0
            };
            let scan_s = pm.scan_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let transform_s = pm.transform_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let output_s = pm.output_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let queue_wait_s = pm.queue_wait_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let send_s = pm.send_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;

            let last_batch_ns = pm.last_batch_time_ns.load(Ordering::Relaxed);

            // Compute batch latency using a consistent snapshot since they are
            // updated at different times. We retry until batches remains the same,
            // capping at 64 attempts to avoid spinning indefinitely under contention.
            // Observability counters only — stale reads are acceptable.
            // Use Relaxed uniformly to match all other load sites in this file.
            let mut latency_batches = pm.batches_total.load(Ordering::Relaxed);
            let mut batch_latency_total;
            let mut attempts = 0;
            loop {
                batch_latency_total = pm.batch_latency_nanos_total.load(Ordering::Relaxed);
                let current_batches = pm.batches_total.load(Ordering::Relaxed);
                if current_batches == latency_batches || attempts >= 64 {
                    latency_batches = current_batches;
                    break;
                }
                latency_batches = current_batches;
                attempts += 1;
            }

            let batch_latency_avg_ns = if latency_batches > 0 {
                batch_latency_total / latency_batches
            } else {
                0
            };
            let inflight = pm.inflight_batches.load(Ordering::Relaxed);
            let backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);
            let transform_health = policy::transform_health(pm);

            pipelines_json.push(format!(
                r#"{"name":"{}","inputs":[{}],"transform":{"sql":"{}","health":"{}","lines_in":{},"lines_out":{},"errors":{},"filter_drop_rate":{:.3}},"outputs":[{}],"batches":{"total":{},"avg_rows":{:.1},"flush_by_size":{},"flush_by_timeout":{},"dropped_batches_total":{},"scan_errors_total":{},"parse_errors_total":{},"last_batch_time_ns":{},"batch_latency_avg_ns":{},"inflight":{},"rows_total":{}},"stage_seconds":{"scan":{:.6},"transform":{:.6},"output":{:.6},"queue_wait":{:.6},"send":{:.6}},"backpressure_stalls":{}}"#,
                esc(&pm.name),
                inputs_json.join(","),
                esc(&pm.transform_sql),
                transform_health.as_str(),
                lines_in,
                lines_out,
                pm.transform_errors.load(Ordering::Relaxed),
                drop_rate,
                outputs_json.join(","),
                batches,
                avg_rows,
                pm.flush_by_size.load(Ordering::Relaxed),
                pm.flush_by_timeout.load(Ordering::Relaxed),
                pm.dropped_batches_total.load(Ordering::Relaxed),
                pm.scan_errors_total.load(Ordering::Relaxed),
                pm.parse_errors_total.load(Ordering::Relaxed),
                last_batch_ns,
                batch_latency_avg_ns,
                inflight,
                batch_rows,
                scan_s,
                transform_s,
                output_s,
                queue_wait_s,
                send_s,
                backpressure,
            ));
        }

        let ready = if policy::is_ready(&self.pipelines) {
            "ready"
        } else {
            "not_ready"
        };
        let component_health = policy::aggregate_component_health(&self.pipelines);
        let ready_reason = policy::ready_reason(&self.pipelines);
        let component_reason = policy::health_reason(component_health);
        let readiness_impact = policy::readiness_impact(component_health);

        format!(
            r#"{"live":{"status":"live","reason":"process_running","observed_at_unix_ns":"{}"},"ready":{"status":"{}","reason":"{}","observed_at_unix_ns":"{}"},"component_health":{"status":"{}","reason":"{}","readiness_impact":"{}","observed_at_unix_ns":"{}"},"pipelines":[{}],"system":{"uptime_seconds":{},"version":"{}"{}}}"#,
            observed_at_unix_ns,
            ready,
            ready_reason,
            observed_at_unix_ns,
            component_health.as_str(),
            component_reason,
            readiness_impact,
            observed_at_unix_ns,
            pipelines_json.join(","),
            uptime,
            VERSION,
            self.memory_json(),
        )
    }

    /// Returns a JSON fragment (starting with a comma) for allocator memory
    /// stats, or an empty string if no stats function is registered.
    fn memory_json(&self) -> String {
        match self.memory_stats_fn.and_then(|f| f()) {
            Some(m) => format!(
                r#","memory":{"resident":{},"allocated":{},"active":{}}"#,
                m.resident, m.allocated, m.active,
            ),
            None => String::new(),
        }
    }"""
status_body_new = """    fn status_body(&self) -> String {
        let uptime = self.start_time.elapsed().as_secs();
        let observed_at_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut pipelines_models = Vec::new();

        for pm in &self.pipelines {
            let inputs_models: Vec<crate::diagnostics::models::InputResponse> = pm
                .inputs
                .iter()
                .map(|(name, typ, stats)| {
                    crate::diagnostics::models::InputResponse {
                        name: name.clone(),
                        typ: typ.clone(),
                        health: stats.health().as_str().to_string(),
                        lines_total: stats.lines(),
                        bytes_total: stats.bytes(),
                        errors: stats.errors(),
                        rotations: stats.rotations(),
                        parse_errors: stats.parse_errors(),
                    }
                })
                .collect();

            let lines_in = pm.transform_in.lines();
            let lines_out: u64 = pm
                .outputs
                .iter()
                .map(|(_, _, s)| s.lines())
                .max()
                .unwrap_or(0);
            let drop_rate = if lines_in > 0 {
                1.0 - (lines_out as f64 / lines_in as f64)
            } else {
                0.0
            };

            let outputs_models: Vec<crate::diagnostics::models::OutputResponse> = pm
                .outputs
                .iter()
                .map(|(name, typ, stats)| {
                    crate::diagnostics::models::OutputResponse {
                        name: name.clone(),
                        typ: typ.clone(),
                        health: stats.health().as_str().to_string(),
                        lines_total: stats.lines(),
                        bytes_total: stats.bytes(),
                        errors: stats.errors(),
                    }
                })
                .collect();

            let batches = pm.batches_total.load(Ordering::Relaxed);
            let batch_rows = pm.batch_rows_total.load(Ordering::Relaxed);
            let avg_rows = if batches > 0 {
                batch_rows as f64 / batches as f64
            } else {
                0.0
            };
            let scan_s = pm.scan_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let transform_s = pm.transform_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let output_s = pm.output_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let queue_wait_s = pm.queue_wait_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;
            let send_s = pm.send_nanos_total.load(Ordering::Relaxed) as f64 / 1e9;

            let last_batch_ns = pm.last_batch_time_ns.load(Ordering::Relaxed);

            let mut latency_batches = pm.batches_total.load(Ordering::Relaxed);
            let mut batch_latency_total;
            let mut attempts = 0;
            loop {
                batch_latency_total = pm.batch_latency_nanos_total.load(Ordering::Relaxed);
                let current_batches = pm.batches_total.load(Ordering::Relaxed);
                if current_batches == latency_batches || attempts >= 64 {
                    latency_batches = current_batches;
                    break;
                }
                latency_batches = current_batches;
                attempts += 1;
            }

            let batch_latency_avg_ns = if latency_batches > 0 {
                batch_latency_total / latency_batches
            } else {
                0
            };
            let inflight = pm.inflight_batches.load(Ordering::Relaxed);
            let backpressure = pm.backpressure_stalls.load(Ordering::Relaxed);
            let transform_health = policy::transform_health(pm);

            pipelines_models.push(crate::diagnostics::models::PipelineResponse {
                name: pm.name.clone(),
                inputs: inputs_models,
                transform: crate::diagnostics::models::TransformResponse {
                    sql: pm.transform_sql.clone(),
                    health: transform_health.as_str().to_string(),
                    lines_in,
                    lines_out,
                    errors: pm.transform_errors.load(Ordering::Relaxed),
                    filter_drop_rate: drop_rate,
                },
                outputs: outputs_models,
                batches: crate::diagnostics::models::BatchesResponse {
                    total: batches,
                    avg_rows,
                    flush_by_size: pm.flush_by_size.load(Ordering::Relaxed),
                    flush_by_timeout: pm.flush_by_timeout.load(Ordering::Relaxed),
                    dropped_batches_total: pm.dropped_batches_total.load(Ordering::Relaxed),
                    scan_errors_total: pm.scan_errors_total.load(Ordering::Relaxed),
                    parse_errors_total: pm.parse_errors_total.load(Ordering::Relaxed),
                    last_batch_time_ns: last_batch_ns,
                    batch_latency_avg_ns,
                    inflight,
                    rows_total: batch_rows,
                },
                stage_seconds: crate::diagnostics::models::StageSecondsResponse {
                    scan: scan_s,
                    transform: transform_s,
                    output: output_s,
                    queue_wait: queue_wait_s,
                    send: send_s,
                },
                backpressure_stalls: backpressure,
            });
        }

        let ready = if policy::is_ready(&self.pipelines) {
            "ready"
        } else {
            "not_ready"
        };
        let component_health = policy::aggregate_component_health(&self.pipelines);
        let ready_reason = policy::ready_reason(&self.pipelines);
        let component_reason = policy::health_reason(component_health);
        let readiness_impact = policy::readiness_impact(component_health);

        let memory_model = self.memory_stats_fn.and_then(|f| f()).map(|m| crate::diagnostics::models::MemoryResponse {
            resident: m.resident,
            allocated: m.allocated,
            active: m.active,
        });

        let response = crate::diagnostics::models::StatusResponse {
            live: crate::diagnostics::models::LiveResponse {
                status: "live".to_string(),
                reason: "process_running".to_string(),
                observed_at_unix_ns: observed_at_unix_ns.to_string(),
            },
            ready: crate::diagnostics::models::ReadyResponse {
                status: ready.to_string(),
                reason: ready_reason.to_string(),
                observed_at_unix_ns: observed_at_unix_ns.to_string(),
            },
            component_health: crate::diagnostics::models::ComponentHealthResponse {
                status: component_health.as_str().to_string(),
                reason: component_reason.to_string(),
                readiness_impact: readiness_impact.to_string(),
                observed_at_unix_ns: observed_at_unix_ns.to_string(),
            },
            pipelines: pipelines_models,
            system: crate::diagnostics::models::SystemResponse {
                uptime_seconds: uptime,
                version: VERSION.to_string(),
                memory: memory_model,
            },
            contract_version: Some(crate::diagnostics::models::DIAGNOSTICS_CONTRACT_VERSION.to_string()),
        };

        serde_json::to_string(&response).unwrap_or_else(|_| "{}".to_string())
    }"""
content = content.replace(status_body_old, status_body_new)

with open("crates/logfwd-io/src/diagnostics.rs", "w") as f:
    f.write(content)
