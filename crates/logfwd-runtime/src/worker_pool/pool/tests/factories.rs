    #[test]
    fn dispatch_prefers_existing_over_spawn() {
        // Even though we could spawn (under limit), we should send to existing worker.
        let states = [ChannelState::HasSpace];
        assert_eq!(dispatch_step(&states, 4), DispatchOutcome::SentToIndex(0));
    }

    // -----------------------------------------------------------------------
    // Pool integration tests (async)
    // -----------------------------------------------------------------------

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use logfwd_diagnostics::diagnostics::ComponentHealth;
    use logfwd_output::sink::{SendResult, Sink, SinkFactory};
    use logfwd_output::{BatchMetadata, ElasticsearchRequestMode, ElasticsearchSinkFactory};
    use std::collections::{BTreeMap, VecDeque};
    use std::fmt;
    use std::future::pending;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};
    use tiny_http::{Header, Response, Server, StatusCode};
    use tracing::{Event, Id, Subscriber};
    use tracing_subscriber::layer::{Context, Layer};
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    /// A sink that counts calls and optionally simulates failures.
    struct CountingSink {
        name: String,
        calls: Arc<AtomicU32>,
        fail: bool,
        fail_shutdown: bool,
    }

    impl Sink for CountingSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let calls = self.calls.clone();
            let fail = self.fail;
            Box::pin(async move {
                calls.fetch_add(1, Ordering::Relaxed);
                if fail {
                    SendResult::IoError(io::Error::other("injected failure"))
                } else {
                    SendResult::Ok
                }
            })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            let fail_shutdown = self.fail_shutdown;
            Box::pin(async move {
                if fail_shutdown {
                    Err(io::Error::other("shutdown failed"))
                } else {
                    Ok(())
                }
            })
        }
    }

    struct CountingSinkFactory {
        calls: Arc<AtomicU32>,
        fail: bool,
        fail_shutdown: bool,
    }

    impl SinkFactory for CountingSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(CountingSink {
                name: "counting".into(),
                calls: Arc::clone(&self.calls),
                fail: self.fail,
                fail_shutdown: self.fail_shutdown,
            }))
        }

        fn name(&self) -> &'static str {
            "counting"
        }
    }

    #[derive(Clone, Debug)]
    enum ScriptedResult {
        Ok,
        RetryAfter(Duration),
        Rejected(&'static str),
    }

    struct ScriptedSink {
        name: String,
        steps: Arc<Mutex<VecDeque<ScriptedResult>>>,
    }

    impl Sink for ScriptedSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let step = self
                .steps
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(ScriptedResult::Ok);
            Box::pin(async move {
                match step {
                    ScriptedResult::Ok => SendResult::Ok,
                    ScriptedResult::RetryAfter(d) => SendResult::RetryAfter(d),
                    ScriptedResult::Rejected(msg) => SendResult::Rejected(msg.to_string()),
                }
            })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct ScriptedSinkFactory {
        name: &'static str,
        steps: Arc<Mutex<VecDeque<ScriptedResult>>>,
    }

    impl ScriptedSinkFactory {
        fn new(name: &'static str, steps: Vec<ScriptedResult>) -> Self {
            Self {
                name,
                steps: Arc::new(Mutex::new(VecDeque::from(steps))),
            }
        }
    }

    impl SinkFactory for ScriptedSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(ScriptedSink {
                name: self.name.to_string(),
                steps: Arc::clone(&self.steps),
            }))
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    struct FailingCreateFactory {
        name: &'static str,
    }

    impl SinkFactory for FailingCreateFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Err(io::Error::other("create failed"))
        }

        fn name(&self) -> &str {
            self.name
        }
    }

    struct HangingSink {
        name: String,
        entered_send: Arc<AtomicBool>,
    }

    impl Sink for HangingSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let entered_send = Arc::clone(&self.entered_send);
            Box::pin(async move {
                entered_send.store(true, Ordering::Release);
                pending::<SendResult>().await
            })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct HangingSinkFactory {
        entered_send: Arc<AtomicBool>,
    }

    impl SinkFactory for HangingSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(HangingSink {
                name: "hanging".into(),
                entered_send: Arc::clone(&self.entered_send),
            }))
        }

        fn name(&self) -> &'static str {
            "hanging"
        }
    }

    struct PanicSink {
        name: String,
        entered_send: Arc<AtomicBool>,
    }

    impl Sink for PanicSink {
        fn send_batch<'a>(
            &'a mut self,
            _batch: &'a RecordBatch,
            _metadata: &'a BatchMetadata,
        ) -> Pin<Box<dyn Future<Output = SendResult> + Send + 'a>> {
            let entered_send = Arc::clone(&self.entered_send);
            Box::pin(async move {
                entered_send.store(true, Ordering::Release);
                panic!("injected send_batch panic");
            })
        }

        fn flush(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }

        fn name(&self) -> &str {
            &self.name
        }

        fn shutdown(&mut self) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send + '_>> {
            Box::pin(async { Ok(()) })
        }
    }

    struct PanicSinkFactory {
        entered_send: Arc<AtomicBool>,
    }

    impl SinkFactory for PanicSinkFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            Ok(Box::new(PanicSink {
                name: "panic".into(),
                entered_send: Arc::clone(&self.entered_send),
            }))
        }

        fn name(&self) -> &'static str {
            "panic"
        }
    }

    struct SlowCreateFactory {
        entered_create: Arc<AtomicBool>,
        release_create: Arc<AtomicBool>,
    }

    impl SinkFactory for SlowCreateFactory {
        fn create(&self) -> io::Result<Box<dyn Sink>> {
            self.entered_create.store(true, Ordering::Release);
            while !self.release_create.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(Box::new(CountingSink {
                name: "slow-create".into(),
                calls: Arc::new(AtomicU32::new(0)),
                fail: false,
                fail_shutdown: false,
            }))
        }

        fn name(&self) -> &'static str {
            "slow-create"
        }
    }

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["hello"]))]).unwrap()
    }

    fn make_metadata() -> BatchMetadata {
        BatchMetadata {
            resource_attrs: Arc::default(),
            observed_time_ns: 0,
        }
    }

    // We need BatchTickets for WorkItems. Since BatchTicket::new is crate-private
    // in logfwd-core, integration tests that need tickets go through pipeline.rs.
    // Here we only test pool mechanics with empty ticket vecs (valid for pool logic,
    // since ticket acking is handled by pipeline.rs separately).
    fn empty_work_item() -> WorkItem {
        WorkItem {
            batch: make_batch(),
            metadata: make_metadata(),
            tickets: vec![],
            num_rows: 0,
            submitted_at: tokio::time::Instant::now(),
            scan_ns: 0,
            transform_ns: 0,
            batch_id: 0,
            span: tracing::Span::none(),
        }
    }

    fn test_metrics() -> Arc<PipelineMetrics> {
        {
            let meter = logfwd_test_utils::test_meter();
            Arc::new(PipelineMetrics::new("test", "", &meter))
        }
    }

    async fn wait_for_flag(flag: &AtomicBool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while !flag.load(Ordering::Acquire) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for flag to become true"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn wait_for_no_active_workers(pool: &OutputWorkerPool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        while pool.output_health.has_active_workers() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for worker slots to clear"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct CapturedSpan {
        name: String,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    struct CapturedEvent {
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct TraceCapture {
        inner: Arc<TraceCaptureInner>,
    }

    #[derive(Default)]
    struct TraceCaptureInner {
        active_spans: Mutex<BTreeMap<String, CapturedSpan>>,
        closed_spans: Mutex<Vec<CapturedSpan>>,
        events: Mutex<Vec<CapturedEvent>>,
    }

    #[derive(Default)]
    struct FieldCapture(BTreeMap<String, String>);

    impl tracing::field::Visit for FieldCapture {
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.0.insert(field.name().to_string(), value.to_string());
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
            self.0
                .insert(field.name().to_string(), format!("{value:?}"));
        }
    }

    impl TraceCapture {
        fn closed_output_span(&self) -> Option<CapturedSpan> {
            self.inner
                .closed_spans
                .lock()
                .expect("closed span mutex poisoned")
                .iter()
                .rev()
                .find(|span| span.name == "output")
                .cloned()
        }

        fn contains_event_message(&self, needle: &str) -> bool {
            self.inner
                .events
                .lock()
                .expect("event mutex poisoned")
                .iter()
                .any(|event| event.fields.get("message").is_some_and(|msg| msg == needle))
        }

        fn contains_event_field(&self, key: &str, needle: &str) -> bool {
            self.inner
                .events
                .lock()
                .expect("event mutex poisoned")
                .iter()
                .any(|event| {
                    event
                        .fields
                        .get(key)
                        .is_some_and(|value| value.contains(needle))
                })
        }
    }

    impl<S> Layer<S> for TraceCapture
    where
        S: Subscriber + for<'lookup> LookupSpan<'lookup>,
    {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &Id,
            _ctx: Context<'_, S>,
        ) {
            let mut visitor = FieldCapture::default();
            attrs.record(&mut visitor);
            self.inner
                .active_spans
                .lock()
                .expect("active span mutex poisoned")
                .insert(
                    format!("{id:?}"),
                    CapturedSpan {
                        name: attrs.metadata().name().to_string(),
                        fields: visitor.0,
                    },
                );
        }

        fn on_record(&self, id: &Id, values: &tracing::span::Record<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldCapture::default();
            values.record(&mut visitor);
            if let Some(span) = self
                .inner
                .active_spans
                .lock()
                .expect("active span mutex poisoned")
                .get_mut(&format!("{id:?}"))
            {
                span.fields.extend(visitor.0);
            }
        }

        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldCapture::default();
            event.record(&mut visitor);
            self.inner
                .events
                .lock()
                .expect("event mutex poisoned")
                .push(CapturedEvent { fields: visitor.0 });
        }

        fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
            if let Some(span) = self
                .inner
                .active_spans
                .lock()
                .expect("active span mutex poisoned")
                .remove(&format!("{id:?}"))
            {
                self.inner
                    .closed_spans
                    .lock()
                    .expect("closed span mutex poisoned")
                    .push(span);
            }
        }
    }

    fn start_elasticsearch_mock(
        status: u16,
        body: &'static str,
        expected_requests: usize,
    ) -> (String, std::thread::JoinHandle<()>) {
        let server = Server::http("127.0.0.1:0").expect("server start failed");
        let addr = match server.server_addr() {
            tiny_http::ListenAddr::IP(addr) => addr,
            _ => panic!("expected IP listen addr"),
        };
        let content_type =
            Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();

        let handle = std::thread::spawn(move || {
            for _ in 0..expected_requests {
                let mut req = match server.recv_timeout(Duration::from_secs(10)) {
                    Ok(Some(req)) => req,
                    Ok(None) => break,
                    Err(_) => break,
                };
                let mut request_body = Vec::new();
                req.as_reader()
                    .read_to_end(&mut request_body)
                    .expect("read request body");
                let response = Response::from_string(body)
                    .with_status_code(StatusCode(status))
                    .with_header(content_type.clone());
                req.respond(response).expect("respond");
            }
        });

        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn pool_spawns_worker_on_first_submit() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        let mut pool = OutputWorkerPool::new(factory, 4, Duration::from_secs(60), test_metrics());
        assert_eq!(pool.worker_count(), 0);

        pool.submit(empty_work_item()).await;
        // Give the worker time to process.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Exactly one worker was created.
        assert_eq!(pool.worker_count(), 1);
        // The sink's send_batch was called once.
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        pool.drain(Duration::from_secs(5)).await;
    }

    #[tokio::test]
    async fn pool_consolidates_work_on_single_worker() {
        let calls = Arc::new(AtomicU32::new(0));
        let factory = Arc::new(CountingSinkFactory {
            calls: Arc::clone(&calls),
            fail: false,
            fail_shutdown: false,
        });
        // max_workers=4, but all items should go to the same worker (MRU).
        let mut pool = OutputWorkerPool::new(factory, 4, Duration::from_secs(60), test_metrics());

        // Submit 3 items sequentially (each after the previous is received).
        for _ in 0..3 {
            pool.submit(empty_work_item()).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Only 1 worker should exist (MRU consolidation).
        assert_eq!(pool.worker_count(), 1);

        pool.drain(Duration::from_secs(5)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }
