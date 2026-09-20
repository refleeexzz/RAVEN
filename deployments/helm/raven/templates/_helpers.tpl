{{/*
RAVEN chart helpers.

Naming strategy: Kubernetes DNS names are a CONTRACT here — raven-config
points at "auth:9081", "broker:9100", "postgres:5432" and so on, and the
NetworkPolicies select pods by the "app" label. So objects keep the plain
component names from the flat manifests (one release per namespace),
instead of the usual release-name prefix.
*/}}

{{- define "raven.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Namespace every object lands in. */}}
{{- define "raven.namespace" -}}
{{- default .Release.Namespace .Values.namespaceOverride -}}
{{- end -}}

{{- define "raven.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels shared by every object. Takes the component name as argument:
  {{ include "raven.labels" (dict "ctx" . "component" "auth") }}
The bare "app: <component>" label is load-bearing: Services select it and
NetworkPolicies match on it. Do not rename.
*/}}
{{- define "raven.labels" -}}
app: {{ .component }}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/part-of: {{ include "raven.name" .ctx }}
app.kubernetes.io/managed-by: {{ .ctx.Release.Service }}
helm.sh/chart: {{ include "raven.chart" .ctx }}
{{- end -}}

{{/* Selector labels (pod template <-> Deployment/Service match). */}}
{{- define "raven.selectorLabels" -}}
app: {{ . }}
{{- end -}}

{{/*
Image reference: global registry + repository:tag, per-component pullPolicy
falling back to the global one.
  {{ include "raven.image" (dict "ctx" . "image" .Values.auth.image) }}
*/}}
{{- define "raven.image" -}}
{{- $registry := .ctx.Values.global.imageRegistry -}}
{{- $sep := ternary "/" "" (ne $registry "") -}}
{{- printf "%s%s%s:%s" $registry $sep .image.repository .image.tag -}}
{{- end -}}

{{- define "raven.imagePullPolicy" -}}
{{- default .ctx.Values.global.imagePullPolicy .image.pullPolicy -}}
{{- end -}}

{{- define "raven.serviceAccountName" -}}
{{- .Values.serviceAccount.name -}}
{{- end -}}

{{/*
Pod security context for RAVEN's own Go services: uid 10001 is baked into
every raven/* image. Third-party images use their own uid (postgres 70,
redis 999, nginx 101, prometheus 65534, grafana 472) — set per template.
*/}}
{{- define "raven.podSecurityContext" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{/* Container security context shared by every workload. */}}
{{- define "raven.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}
