/**
 * RAVEN JavaScript SDK — official client for the RAVEN distributed jobs
 * platform. Zero dependencies, Node.js >= 18 (native fetch).
 *
 * @example
 * import { RavenClient } from 'raven-sdk';
 *
 * const client = new RavenClient({ baseUrl: 'http://localhost:8080' });
 * await client.login('me@example.com', 'correct horse battery');
 * const job = await client.createJob({ type: 'webhook', payload: { url: 'https://me.example/hook' } });
 * const final = await client.watchJob(job.id);
 * console.log(final.status);
 */

export { RavenClient, DEFAULT_BASE_URL, DEFAULT_TIMEOUT_MS, isTerminalStatus } from './client.js';
export { RavenError, TimeoutError, TransportError } from './errors.js';
