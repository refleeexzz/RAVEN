/**
 * Typed errors for the RAVEN JavaScript SDK.
 *
 * Every API failure mirrors the gateway's error envelope:
 *   {"error": {"code": "job_not_found", "message": "...", "request_id": "..."}}
 */

/** An error returned by the RAVEN API (or a transport layer below it). */
export class RavenError extends Error {
  /**
   * @param {object} init
   * @param {string} init.code Stable snake_case machine code, e.g. "job_not_found".
   * @param {string} init.message Client-safe prose.
   * @param {string} [init.requestId] Matches request_id in the gateway logs.
   * @param {number} [init.statusCode] HTTP status (0 when no response arrived).
   * @param {unknown} [init.cause] Underlying error, for transport failures.
   */
  constructor({ code, message, requestId, statusCode = 0, cause }) {
    super(message, { cause });
    this.name = 'RavenError';
    /** @type {string} stable machine code */
    this.code = code;
    /** @type {string | undefined} gateway log correlation id */
    this.requestId = requestId;
    /** @type {number} HTTP status of the response */
    this.statusCode = statusCode;
  }

  /** Human-readable one-liner quoting the request id when present. */
  toString() {
    let s = `raven: ${this.code}: ${this.message}`;
    if (this.statusCode) s += ` (status ${this.statusCode})`;
    if (this.requestId) s += ` [request ${this.requestId}]`;
    return s;
  }

  /**
   * True when err is a RavenError with the given machine code.
   * @param {unknown} err
   * @param {string} code
   */
  static isCode(err, code) {
    return err instanceof RavenError && err.code === code;
  }
}

/** A failure before any HTTP response arrived: DNS, refused socket, timeout. */
export class TransportError extends RavenError {
  /** @param {string} message @param {unknown} [cause] */
  constructor(message, cause) {
    super({ code: 'transport_error', message, cause });
    this.name = 'TransportError';
  }
}

/** The request exceeded the configured timeout. */
export class TimeoutError extends RavenError {
  /** @param {string} message @param {unknown} [cause] */
  constructor(message, cause) {
    super({ code: 'timeout', message, cause });
    this.name = 'TimeoutError';
  }
}
