# Integration with VictoriaMetrics log-collectors-benchmark
# Run from the memagent repo root:
#   make -f deploy/Makefile bench-up-ff
#   make -f deploy/Makefile bench-down-ff

CLUSTER_NAME ?= log-collectors-bench

build-ff:
	docker build -t ff:latest .
	kind load docker-image --name $(CLUSTER_NAME) ff:latest

bench-up-ff: build-ff
	kubectl create namespace collectors 2>/dev/null || true
	kubectl apply -f deploy/configmap.yml
	kubectl apply -f deploy/rbac.yml
	kubectl apply -f deploy/daemonset.yml

bench-down-ff:
	kubectl delete -f deploy/daemonset.yml 2>/dev/null || true
	kubectl delete -f deploy/rbac.yml 2>/dev/null || true
	kubectl delete -f deploy/configmap.yml 2>/dev/null || true

# Quick test: verify the daemon starts and connects
test-ff:
	kubectl logs -n collectors -l app=ff --tail=20

# Full benchmark workflow (assumes VM benchmark repo is cloned at /tmp/vm-bench):
#
# 1. Start the VM benchmark infrastructure:
#    cd /tmp/vm-bench && make bench-up-monitoring
#
# 2. Deploy all VM benchmark collectors:
#    cd /tmp/vm-bench && make bench-up-collectors
#
# 3. Deploy our collector:
#    make -f deploy/Makefile bench-up-ff
#
# 4. Start the log generator with ramp-up:
#    cd /tmp/vm-bench && make bench-up-generator RAMP_UP_STEP=5 RAMP_UP_STEP_INTERVAL=1s GENERATOR_REPLICAS=10
#
# 5. Watch Grafana:
#    kubectl port-forward -n monitoring svc/vms-grafana 3000:80
#    Open http://localhost:3000 (admin/admin)
#
# 6. Cleanup:
#    cd /tmp/vm-bench && make bench-down-generator
#    make -f deploy/Makefile bench-down-ff
#    cd /tmp/vm-bench && make bench-down-all
