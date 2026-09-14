#!/usr/bin/env python3
"""Generates deployments/kubernetes/observability.yaml.

The Grafana dashboard JSON is embedded into a ConfigMap, so we read the
real file from disk instead of hand-copying it — no transcription drift.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "deployments" / "kubernetes" / "observability.yaml"
DASHBOARD = (ROOT / "deployments" / "grafana" / "dashboards" / "raven-platform.json").read_text(encoding="utf-8").rstrip()
DASHBOARD_INDENTED = "\n".join("    " + line for line in DASHBOARD.splitlines())

HEADER = '''# RAVEN observability for Docker Desktop k8s — Prometheus + Grafana +
# Jaeger in a single apply. Mirrors the compose setup: same scrape config
# (Service DNS names match compose service names), same provisioning,
# same dashboard. UIs via port-forward:
#   kubectl -n raven port-forward svc/grafana 3000:3000
#   kubectl -n raven port-forward svc/jaeger 16686:16686
'''

PROMETHEUS = '''---
# Prometheus scrape config as a ConfigMap so it can be edited with a
# plain `kubectl edit` + pod restart.
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: raven
  labels:
    app.kubernetes.io/part-of: raven
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    scrape_configs:
      # Prometheus watches itself so the stack is never a black box.
      - job_name: prometheus
        static_configs:
          - targets: ["localhost:9090"]
      - job_name: gateway
        static_configs:
          - targets: ["gateway:8080"]
      - job_name: auth
        static_configs:
          - targets: ["auth:8081"]
      - job_name: users
        static_configs:
          - targets: ["users:8082"]
      - job_name: jobs
        static_configs:
          - targets: ["jobs:8083"]
      - job_name: websocket
        static_configs:
          - targets: ["websocket:8084"]
      - job_name: worker
        static_configs:
          - targets: ["worker:8085"]
      - job_name: broker
        static_configs:
          - targets: ["broker:9101"]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: raven
  labels:
    app: prometheus
    app.kubernetes.io/part-of: raven
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      # No pod in this namespace calls the k8s API: dedicated SA, token off.
      serviceAccountName: raven
      automountServiceAccountToken: false
      securityContext:
        # The prometheus image already runs as nobody (uid 65534).
        runAsNonRoot: true
        runAsUser: 65534
        runAsGroup: 65534
        fsGroup: 65534   # makes the /prometheus emptyDir writable
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: prometheus
          image: prom/prometheus:v3.4.1
          securityContext:
            allowPrivilegeEscalation: false
            # TSDB lives on the /prometheus emptyDir; root fs read-only.
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          ports:
            - containerPort: 9090
          # Dev only: no PVC. Losing metrics history on restart is fine
          # on a laptop and keeps the setup simple.
          readinessProbe:
            httpGet:
              path: /-/ready
              port: 9090
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /-/healthy
              port: 9090
            initialDelaySeconds: 15
            periodSeconds: 20
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 512Mi
          volumeMounts:
            - name: config
              mountPath: /etc/prometheus/prometheus.yml
              subPath: prometheus.yml
            - name: tsdb
              mountPath: /prometheus
      volumes:
        - name: config
          configMap:
            name: prometheus-config
        - name: tsdb
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: raven
  labels:
    app: prometheus
    app.kubernetes.io/part-of: raven
spec:
  # DEV ONLY: LoadBalancer so Docker Desktop binds the UI to localhost:9090.
  # In production expose it (if at all) behind an authenticated Ingress —
  # the Prometheus UI has no login and answers every query.
  type: LoadBalancer
  selector:
    app: prometheus
  ports:
    - name: http
      port: 9090
      targetPort: 9090
'''

GRAFANA_HEAD = '''---
# Grafana provisioning, embedded: datasources + dashboard provider.
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-provisioning
  namespace: raven
  labels:
    app.kubernetes.io/part-of: raven
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
      - name: Prometheus
        uid: prometheus
        type: prometheus
        access: proxy
        url: http://prometheus:9090
        isDefault: true
      - name: Jaeger
        uid: jaeger
        type: jaeger
        access: proxy
        url: http://jaeger:16686
  dashboards.yaml: |
    apiVersion: 1
    providers:
      - name: raven
        orgId: 1
        folder: RAVEN
        type: file
        disableDeletion: false
        updateIntervalSeconds: 10
        allowUiUpdates: true
        options:
          path: /etc/grafana/dashboards
          foldersFromFilesStructure: false
---
# The RAVEN Platform Status dashboard, embedded from
# deployments/grafana/dashboards/raven-platform.json (single source of
# truth — regenerate this file with deployments/gen-observability-k8s.py).
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-dashboard-raven
  namespace: raven
  labels:
    app.kubernetes.io/part-of: raven
data:
  raven-platform.json: |
'''

GRAFANA_TAIL = '''---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  namespace: raven
  labels:
    app: grafana
    app.kubernetes.io/part-of: raven
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana
  template:
    metadata:
      labels:
        app: grafana
    spec:
      # No pod in this namespace calls the k8s API: dedicated SA, token off.
      serviceAccountName: raven
      automountServiceAccountToken: false
      securityContext:
        # The grafana image's own user is uid 472.
        runAsNonRoot: true
        runAsUser: 472
        runAsGroup: 472
        fsGroup: 472   # makes the /var/lib/grafana emptyDir writable
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: grafana
          image: grafana/grafana:11.6.0
          securityContext:
            allowPrivilegeEscalation: false
            # Grafana state (sqlite, sessions) goes to the emptyDir;
            # root fs read-only.
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          ports:
            - containerPort: 3000
          env:
            # Dev-only login. Change it if you expose this beyond localhost.
            - name: GF_SECURITY_ADMIN_USER
              value: admin
            - name: GF_SECURITY_ADMIN_PASSWORD
              value: admin
          readinessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /api/health
              port: 3000
            initialDelaySeconds: 20
            periodSeconds: 20
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 512Mi
          volumeMounts:
            - name: provisioning
              mountPath: /etc/grafana/provisioning
            - name: dashboards
              mountPath: /etc/grafana/dashboards
            - name: grafana-data
              mountPath: /var/lib/grafana
      volumes:
        - name: provisioning
          configMap:
            name: grafana-provisioning
            items:
              - key: datasources.yaml
                path: datasources/datasources.yaml
              - key: dashboards.yaml
                path: dashboards/dashboards.yaml
        - name: dashboards
          configMap:
            name: grafana-dashboard-raven
        - name: grafana-data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: grafana
  namespace: raven
  labels:
    app: grafana
    app.kubernetes.io/part-of: raven
spec:
  # DEV ONLY: LoadBalancer so Docker Desktop binds the UI to localhost:3000.
  # In production put it behind an authenticated Ingress and change the
  # admin/admin login below.
  type: LoadBalancer
  selector:
    app: grafana
  ports:
    - name: http
      port: 3000
      targetPort: 3000
'''

JAEGER = '''---
# Jaeger all-in-one: collector + query + UI in one pod. Perfect for dev,
# not for production.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger
  namespace: raven
  labels:
    app: jaeger
    app.kubernetes.io/part-of: raven
spec:
  replicas: 1
  selector:
    matchLabels:
      app: jaeger
  template:
    metadata:
      labels:
        app: jaeger
    spec:
      # No pod in this namespace calls the k8s API: dedicated SA, token off.
      serviceAccountName: raven
      automountServiceAccountToken: false
      securityContext:
        # The jaeger all-in-one image already runs as uid 10001.
        runAsNonRoot: true
        runAsUser: 10001
        seccompProfile:
          type: RuntimeDefault
      containers:
        - name: jaeger
          image: jaegertracing/all-in-one:1.67.0
          securityContext:
            allowPrivilegeEscalation: false
            # In-memory store writes nothing to disk: fully read-only.
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          ports:
            - name: ui
              containerPort: 16686
            - name: otlp-grpc
              containerPort: 4317
          env:
            # Without this the all-in-one image doesn't open the OTLP
            # receiver and our traces would have nowhere to go.
            - name: COLLECTOR_OTLP_ENABLED
              value: "true"
          readinessProbe:
            httpGet:
              path: /
              port: ui
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /
              port: ui
            initialDelaySeconds: 15
            periodSeconds: 20
          resources:
            # Jaeger keeps spans in memory. 100% sampling + a load test
            # OOM-killed it at 512Mi (exit 137, spans wiped). 1Gi + keep
            # an eye on it; drop the sampling ratio before bigger tests.
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 500m
              memory: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: jaeger
  namespace: raven
  labels:
    app: jaeger
    app.kubernetes.io/part-of: raven
spec:
  # DEV ONLY: LoadBalancer so Docker Desktop binds the UI to localhost:16686
  # and host-run services can push OTLP to localhost:4317. In production the
  # UI belongs behind an authenticated Ingress; the collector stays internal.
  type: LoadBalancer
  selector:
    app: jaeger
  ports:
    - name: ui
      port: 16686
      targetPort: ui
    - name: otlp-grpc
      port: 4317
      targetPort: otlp-grpc
'''

OUT.write_text(
    HEADER + PROMETHEUS + GRAFANA_HEAD + DASHBOARD_INDENTED + "\n" + GRAFANA_TAIL + JAEGER,
    encoding="utf-8",
    newline="\n",   # repo policy is LF everywhere (.gitattributes: eol=lf)
)
print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")
