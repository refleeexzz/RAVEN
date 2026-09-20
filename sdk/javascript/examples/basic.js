#!/usr/bin/env node
// A small tour of the RAVEN JavaScript SDK against a live local stack
// (docker compose up). Registers a throwaway user, submits a webhook job,
// watches it to a terminal state and lists workers.
//
// Run from the sdk/javascript directory:
//
//   node examples/basic.js --email demo@example.com --password demo12345

import { RavenClient, RavenError } from '../src/index.js';

function parseArgs(argv) {
  const args = { base: 'http://localhost:8080', email: 'demo@example.com', password: 'demo12345' };
  for (let i = 2; i < argv.length; i += 2) {
    const key = argv[i].replace(/^--/, '');
    args[key] = argv[i + 1];
  }
  return args;
}

const args = parseArgs(process.argv);
const client = new RavenClient({ baseUrl: args.base });

// Register is idempotent-friendly for demos: if the email is taken we
// just log in with the same credentials.
try {
  await client.register({ email: args.email, password: args.password, displayName: 'SDK Demo' });
} catch (err) {
  console.log(`register: ${err} (continuing to login)`);
}

await client.login(args.email, args.password);
console.log('logged in');

const job = await client.createJob({
  type: 'webhook',
  payload: { url: 'https://example.com/hook', event: 'sdk.demo' },
});
console.log(`job ${job.id} created, status ${job.status}`);

const final = await client.watchJob(job.id, { intervalMs: 2000, timeoutMs: 120_000 });
console.log(`job ${final.id} finished as ${final.status}`);

const { workers } = await client.listWorkers();
console.log(`${workers.length} live worker(s)`);

const report = await client.healthServices();
for (const svc of report.services) {
  console.log(`  ${svc.name.padEnd(12)} ${svc.status.padEnd(9)} ${svc.detail ?? ''}`);
}

await client.logout();
console.log('logged out');
