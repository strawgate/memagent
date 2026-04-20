//! Minimal fake Kubernetes API server for vlagent benchmarks.
//!
//! Serves just enough endpoints for vlagent's kubernetesCollector to discover
//! one pod with one container, then tail its log file from disk.

use std::sync::Arc;

pub struct FakeK8sConfig {
    pub pod_name: String,
    pub namespace: String,
    pub container_name: String,
    pub container_id: String,
    pub node_name: String,
}

/// A running fake K8s API server. Stops when dropped.
pub struct FakeK8sApi {
    _server: Arc<tiny_http::Server>,
    _handle: std::thread::JoinHandle<()>,
}

impl FakeK8sApi {
    pub fn start(addr: &str, config: FakeK8sConfig) -> Result<Self, String> {
        let server =
            Arc::new(tiny_http::Server::http(addr).map_err(|e| format!("fake K8s API: {e}"))?);

        let srv = Arc::clone(&server);
        let handle = std::thread::spawn(move || serve(srv, config));

        Ok(FakeK8sApi {
            _server: server,
            _handle: handle,
        })
    }
}

const POD_UID: &str = "bench-uid-00000000-0000-0000-0000-000000000000";

fn serve(server: Arc<tiny_http::Server>, cfg: FakeK8sConfig) {
    let json_header =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();

    let node_obj = serde_json::json!({
        "kind": "Node", "apiVersion": "v1",
        "metadata": {"name": &cfg.node_name},
        "spec": {},
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    });

    let pod_obj = serde_json::json!({
        "kind": "Pod", "apiVersion": "v1",
        "metadata": {
            "name": &cfg.pod_name,
            "namespace": &cfg.namespace,
            "uid": POD_UID,
        },
        "spec": {
            "nodeName": &cfg.node_name,
            "containers": [{
                "name": &cfg.container_name,
                "image": "bench:latest",
            }],
            "initContainers": [],
        },
        "status": {
            "phase": "Running",
            "containerStatuses": [{
                "name": &cfg.container_name,
                "containerID": format!("containerd://{}", &cfg.container_id),
                "state": {"running": {"startedAt": "2024-01-01T00:00:00Z"}},
                "ready": true,
            }],
        },
    });

    let ns_obj = serde_json::json!({
        "kind": "Namespace", "apiVersion": "v1",
        "metadata": {"name": &cfg.namespace},
    });

    let version_obj = serde_json::json!({
        "major": "1", "minor": "30", "gitVersion": "v1.30.0-fake"
    });

    let node_list = make_list("NodeList", &[&node_obj]);
    let pod_list = make_list("PodList", &[&pod_obj]);
    let ns_list = make_list("NamespaceList", &[&ns_obj]);
    let empty_list = make_list("List", &[]);

    let node_path = format!("/api/v1/nodes/{}", cfg.node_name);
    let proxy_pods_path = format!("/api/v1/nodes/{}/proxy/pods", cfg.node_name);
    let single_pod_path = format!("/api/v1/namespaces/{}/pods/{}", cfg.namespace, cfg.pod_name);
    let single_ns_path = format!("/api/v1/namespaces/{}", cfg.namespace);

    for mut request in server.incoming_requests() {
        let url = request.url().to_string();
        let path = url.split('?').next().unwrap_or(&url);
        let is_watch = url.contains("watch=true") || url.contains("watch=1");

        // Drain request body to avoid broken pipe.
        if *request.method() != tiny_http::Method::Get {
            let mut sink = Vec::new();
            let _ = request.as_reader().read_to_end(&mut sink);
        }

        let body = if is_watch {
            // Watch: send ADDED event then keep connection alive.
            let event = serde_json::json!({"type": "ADDED", "object": &pod_obj});
            let event_str = format!("{}\n", serde_json::to_string(&event).unwrap());
            let resp = tiny_http::Response::from_string(&event_str)
                .with_status_code(200)
                .with_header(json_header.clone());
            let _ = request.respond(resp);
            // Connection closes when tiny_http drops the response — that's fine,
            // vlagent will reconnect. This avoids blocking the thread.
            continue;
        } else if path == "/api/v1/nodes" {
            &node_list
        } else if path == node_path {
            &serde_json::to_string(&node_obj).unwrap()
        } else if path == proxy_pods_path || path == "/api/v1/pods" {
            &pod_list
        } else if path == single_pod_path {
            &serde_json::to_string(&pod_obj).unwrap()
        } else if path == "/api/v1/namespaces" {
            &ns_list
        } else if path == single_ns_path {
            &serde_json::to_string(&ns_obj).unwrap()
        } else if matches!(
            path,
            "/api" | "/api/v1" | "/version" | "/healthz" | "/readyz" | "/livez"
        ) {
            &serde_json::to_string(&version_obj).unwrap()
        } else {
            &empty_list
        };

        let resp = tiny_http::Response::from_string(body.clone())
            .with_status_code(200)
            .with_header(json_header.clone());
        let _ = request.respond(resp);
    }
}

fn make_list(kind: &str, items: &[&serde_json::Value]) -> String {
    serde_json::to_string(&serde_json::json!({
        "kind": kind,
        "apiVersion": "v1",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }))
    .unwrap()
}
