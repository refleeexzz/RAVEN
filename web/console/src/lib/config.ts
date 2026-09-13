// Endpoint configuration. Override at build/dev time with VITE_* env vars.
const env = import.meta.env as Record<string, string | undefined>;

export const config = {
  gatewayUrl: env.VITE_GATEWAY_URL ?? "http://localhost:8080",
  brokerUrl: env.VITE_BROKER_URL ?? "http://localhost:9101",
  realtimeUrl: env.VITE_REALTIME_URL ?? "http://localhost:8084",
  wsUrl: env.VITE_WS_URL ?? "ws://localhost:8084/ws",
  grafanaUrl: env.VITE_GRAFANA_URL ?? "http://localhost:3000",
  jaegerUrl: env.VITE_JAEGER_URL ?? "http://localhost:16686",
  prometheusUrl: env.VITE_PROMETHEUS_URL ?? "http://localhost:9090",
} as const;
