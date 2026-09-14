import { config } from "./config";

// Thin REST client for the gateway (http://localhost:8080).

export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(
  url: string,
  init: RequestInit & { token?: string | null } = {},
): Promise<T> {
  const { token, ...rest } = init;
  const headers = new Headers(rest.headers);
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (rest.body && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");

  let res: Response;
  try {
    res = await fetch(url, { ...rest, headers, signal: rest.signal ?? AbortSignal.timeout(4000) });
  } catch (err) {
    throw new ApiError(err instanceof Error ? err.message : "Network error", 0);
  }
  if (!res.ok) {
    let detail = res.statusText;
    try {
      // The gateway sends {"error":{"code","message","request_id"}}; some
      // middleware (timeout/recovery) send a bare {"error":"..."}. Handle both.
      const body = (await res.json()) as {
        error?: string | { code?: string; message?: string };
        message?: string;
      };
      if (typeof body.error === "string") {
        detail = body.error;
      } else if (body.error && typeof body.error === "object" && body.error.message) {
        detail = body.error.message;
      } else if (body.message) {
        detail = body.message;
      }
    } catch {
      // non-JSON error body — keep statusText
    }
    throw new ApiError(detail || `HTTP ${res.status}`, res.status);
  }
  return (await res.json()) as T;
}

export interface LoginResponse {
  access_token: string;
  token_type?: string;
  expires_in?: number;
}

export async function login(email: string, password: string): Promise<LoginResponse> {
  const data = await request<{ access_token?: string; token?: string } & Record<string, unknown>>(
    `${config.gatewayUrl}/api/auth/login`,
    { method: "POST", body: JSON.stringify({ email, password }) },
  );
  const token = data.access_token ?? data.token;
  if (!token) throw new ApiError("Login response did not include an access token", 200);
  return { ...data, access_token: token };
}

export async function getJson<T>(url: string, token?: string | null): Promise<T> {
  return request<T>(url, { token });
}

export async function postJson<T>(
  url: string,
  body: unknown,
  opts: { token?: string | null; idempotencyKey?: string } = {},
): Promise<T> {
  const headers: Record<string, string> = {};
  if (opts.idempotencyKey) headers["Idempotency-Key"] = opts.idempotencyKey;
  return request<T>(url, {
    method: "POST",
    body: JSON.stringify(body),
    token: opts.token,
    headers,
  });
}

/** Quick reachability check used to decide live vs demo mode.
 *  Uses /health: it is public, so a fresh visitor without a token still
 *  detects a running gateway instead of falling into demo mode. */
export async function probeGateway(): Promise<boolean> {
  try {
    const res = await fetch(`${config.gatewayUrl}/health`, {
      signal: AbortSignal.timeout(2500),
    });
    return res.ok;
  } catch {
    return false;
  }
}
