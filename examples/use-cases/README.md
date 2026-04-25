# Use-case configuration examples

This folder contains 20 starter configurations for common collection targets (Redis, PostgreSQL, Kubernetes, syslog, etc.).

## How to use

1. Copy the closest file to your environment, e.g. `cp examples/use-cases/redis-to-otlp.yaml ffwd.yaml`.
2. Edit input paths, output endpoints, and credentials.
3. Validate before running:

```bash
ff validate --config ffwd.yaml
```

If you want to inspect expansion and normalization before editing host-specific
paths, run:

```bash
ff effective-config --config ffwd.yaml
```

## Included examples

1. `apache-to-elasticsearch.yaml`
2. `app-json-errors-only-to-otlp.yaml`
3. `docker-json-to-otlp.yaml`
4. `kafka-to-otlp.yaml`
5. `kubernetes-cri-to-loki.yaml`
6. `kubernetes-cri-to-otlp.yaml`
7. `linux-auth-to-loki.yaml`
8. `mongodb-to-otlp.yaml`
9. `mysql-to-otlp.yaml`
10. `nginx-access-to-loki.yaml`
11. `nginx-error-to-otlp.yaml`
12. `otlp-in-to-elasticsearch.yaml`
13. `otlp-in-to-loki.yaml`
14. `postgresql-to-otlp.yaml`
15. `rabbitmq-to-otlp.yaml`
16. `redis-to-otlp.yaml`
17. `security-audit-to-file.yaml`
18. `syslog-tcp-to-elasticsearch.yaml`
19. `syslog-udp-to-otlp.yaml`
20. `system-journal-export-to-otlp.yaml`

The CLI wizard (`ff wizard`) uses the same template ideas so examples and generated configs stay aligned.
