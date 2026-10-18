import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { LocalConfig } from '../types.js';
import { syncWorkflowDefinitions } from '../workflow/cache.js';
import { leaseNext, reportAttempt, startRun } from '../workflow/engine.js';
import { indexedPosition, statesIn, topScope } from '../workflow/scope.js';
import { readRun, runFilePath, writeRun } from '../workflow/store.js';
import { RUN_RECONCILE_CHECKSUM_VERSION, RUN_RECONCILE_VERSION, type DriverIdentity } from '../workflow/types.js';

// Settling reads a step instance's scope from its path, so the number of paths parsed counts settle work without a clock.
const counter = vi.hoisted(() => ({ owners: 0, parsed: 0, queue: 0, read: 0, status: 0 }));
vi.mock('../workflow/step-path.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../workflow/step-path.js')>();
  return {
    ...actual,
    parseStepPath: (value: string) => {
      counter.parsed += 1;
      return actual.parseStepPath(value);
    },
  };
});

// Every scan over a run's step instances reads each one's path, so the number of `id` reads counts how many it looks at.
// Counting the instances started against `max_parallel` reads only their `status`, which `status` reads count.
vi.mock('../workflow/store.js', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../workflow/store.js')>();
  return {
    ...actual,
    readRun: async (...args: Parameters<typeof actual.readRun>) => {
      const run = await actual.readRun(...args);
      if (typeof run === 'string') return run;
      run.steps = run.steps.map((state) => new Proxy(state, {
        get: (target, key, receiver) => {
          if (key === 'id') counter.read += 1;
          if (key === 'status') counter.status += 1;
          return Reflect.get(target, key, receiver);
        },
      }));
      const fanOuts = run.reconcile?.fanOuts;
      for (const progress of Object.values(fanOuts ?? {})) {
        progress.queue = new Proxy(progress.queue, {
          get: (target, key, receiver) => {
            if (key === 'filter' || key === 'shift' || key === 'sort' || key === 'splice' || key === 'unshift') {
              counter.queue += target.length;
            } else if (key === 'pop' || key === 'push') {
              counter.queue += 1;
            }
            return Reflect.get(target, key, receiver);
          },
        });
      }
      if (run.reconcile && fanOuts) {
        run.reconcile.fanOuts = new Proxy(fanOuts, {
          get: (target, key, receiver) => {
            counter.owners += 1;
            return Reflect.get(target, key, receiver);
          },
          ownKeys: (target) => {
            const keys = Reflect.ownKeys(target);
            counter.owners += keys.length;
            return keys;
          },
        });
      }
      return run;
    },
  };
});

const dirs: string[] = [];
afterEach(() => {
  for (const dir of dirs.splice(0)) fs.rmSync(dir, { recursive: true, force: true });
});

const driver: DriverIdentity = { kind: 'local', machine: 'a', workspace: '/work' };
const CHECKSUM_ALGORITHM = 'sha256';
const FAKE_OWNER_PREFIX = 'other';
const FIRST_ITEM_ID = 'build[0]';
const FUTURE_CHECKSUM_VERSION = RUN_RECONCILE_CHECKSUM_VERSION + 1;
const FUTURE_RECONCILE_VERSION = RUN_RECONCILE_VERSION + 1;
const LARGE_OWNER_COUNT = 200;
const MAX_OWNER_REGISTRY_ACCESSES = 5;
const SMALL_OWNER_COUNT = 100;
const STALE_ITEM_ID = 'build[99]';

const FAN = `schemaVersion: v1alpha1
name: fan
description: An uncapped for_each over an inline workflow.
inputs:
  xs: [number]
steps:
  - id: build
    for_each: inputs.xs
    workflow:
      steps:
        - id: compile
          command: node compile.js
        - id: test
          needs: [compile]
          command: node test.js
  - id: after
    needs: [build]
    command: node after.js
`;

/** The fan workflow with a workflow inside each item's workflow, so starting an item adds instances twice. */
const NESTED_FAN = `schemaVersion: v1alpha1
name: fan
description: An uncapped for_each over an inline workflow that runs another.
inputs:
  xs: [number]
steps:
  - id: build
    for_each: inputs.xs
    workflow:
      steps:
        - id: inner
          workflow:
            steps:
              - id: compile
                command: node compile.js
        - id: test
          needs: [inner]
          command: node test.js
  - id: after
    needs: [build]
    command: node after.js
`;

/** An uncapped for_each whose items each run after the one before them. */
const CHAIN = `schemaVersion: v1alpha1
name: fan
description: An uncapped for_each over a chain of items.
inputs:
  xs: [{ id: string }]
steps:
  - id: build
    for_each: inputs.xs
    command: node build.js
`;

/** A for_each that runs at most two items at a time. */
const CAPPED = `schemaVersion: v1alpha1
name: fan
description: A capped for_each.
inputs:
  xs: [number]
steps:
  - id: build
    for_each: { items: inputs.xs, max_parallel: 2 }
    command: node build.js
`;

/** Independent fan-outs whose ids include an inherited Object property. */
const PROTOTYPE_FANS = `schemaVersion: v1alpha1
name: fan
description: Fan-outs with prototype-sensitive ids.
inputs:
  xs: [number]
steps:
  - id: alpha
    for_each: inputs.xs
    command: node alpha.js
  - id: constructor
    needs: []
    for_each: inputs.xs
    command: node constructor.js
`;

/** A run of a fan workflow over `items` items that no `next` has advanced yet. */
async function startFan(items: number, body = FAN, item = (index: number): unknown => index): Promise<{ config: LocalConfig; run: string }> {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'teamai-workflow-settle-cost-'));
  dirs.push(root);
  const knowledge = path.join(root, 'knowledge');
  fs.mkdirSync(path.join(knowledge, 'workflows'), { recursive: true });
  fs.writeFileSync(path.join(knowledge, 'workflows', 'fan.yaml'), body);
  const config: LocalConfig = {
    repo: { localPath: knowledge, remote: 'local', kind: 'self' }, username: 'tester', scope: 'project',
    additionalRoles: [], projectRoot: root, dataHome: path.join(root, 'data'), usageReportEnabled: false,
  };
  await syncWorkflowDefinitions(config);
  const started = await startRun(config, 'fan', { xs: Array.from({ length: items }, (_, index) => item(index)) });
  if ('error' in started) throw new Error(started.error.message);
  return { config, run: String(started.data.run) };
}

/** The paths parsed by the first `next` of a run over `items` items, which starts every item and leases `first`. */
async function firstNextWork(items: number, body: string, first: string): Promise<number> {
  const { config, run } = await startFan(items, body);
  counter.parsed = 0;
  const next = await leaseNext(config, run, driver);
  expect(next).toMatchObject({ data: { lease: { step: first } } });
  return counter.parsed;
}

/** The step instances looked at per `next` and `report` while one driver runs every step of a run over `items` items. */
async function wholeRunWork(items: number): Promise<{ parsed: number; queue: number }> {
  const { config, run } = await startFan(items);
  counter.parsed = 0;
  counter.queue = 0;
  let calls = 0;
  for (;;) {
    const next = await leaseNext(config, run, driver);
    calls += 1;
    const lease = 'data' in next ? next.data.lease as { attempt: string; revision: number } | undefined : undefined;
    if (!lease) {
      expect(next).toMatchObject({ data: { done: { outcome: 'succeeded' } } });
      break;
    }
    const report = await reportAttempt(config, run, { attempt: lease.attempt, revision: lease.revision, outcome: 'success', driver });
    expect(report).toMatchObject({ data: { accepted: true } });
    calls += 1;
  }
  // A lease and a report for each item's two steps and for `after`, then the `next` that answers done.
  expect(calls).toBe(2 * (2 * items + 1) + 1);
  return { parsed: counter.parsed / calls, queue: counter.queue / calls };
}

/** Owner-registry work to report through one fan-out while unrelated fan-outs are persisted. */
async function ownerLookupWork(otherOwners: number): Promise<number> {
  const { config, run } = await startFan(2);
  const first = await leaseNext(config, run, driver);
  const lease = 'data' in first ? first.data.lease as { attempt: string; revision: number } | undefined : undefined;
  expect(lease).toBeDefined();
  const loaded = await readRun(config, run);
  if (typeof loaded === 'string') throw new Error(`run was ${loaded}`);
  for (let index = 0; index < otherOwners; index += 1) {
    loaded.reconcile!.fanOuts[`${FAKE_OWNER_PREFIX}${index}`] = { queue: [], queued: {}, remaining: 1 };
  }
  await writeRun(config, loaded);
  counter.owners = 0;
  expect(await reportAttempt(config, run, { attempt: lease!.attempt, revision: lease!.revision, outcome: 'success', driver }))
    .toMatchObject({ data: { accepted: true } });
  return counter.owners;
}

/**
 * The step instances looked at by the `next` that skips a chain of `items` items behind the failed item at its head.
 * Forward, each item runs after the one before it; in reverse, after the one after it.
 */
async function skippedChainWork(items: number, order: 'forward' | 'reverse'): Promise<number> {
  const head = order === 'forward' ? 0 : items - 1;
  const { config, run } = await startFan(items, CHAIN, (index) => (index === head
    ? { id: `i${index}` }
    : { id: `i${index}`, after: [`i${order === 'forward' ? index - 1 : index + 1}`] }));
  const first = await leaseNext(config, run, driver);
  const lease = 'data' in first ? first.data.lease as { step: string; attempt: string; revision: number } : undefined;
  expect(lease?.step).toBe(`build[${head}]`);
  await reportAttempt(config, run, { attempt: lease!.attempt, revision: lease!.revision, outcome: 'failed', driver });
  counter.read = 0;
  expect(await leaseNext(config, run, driver)).toMatchObject({ data: { done: { outcome: 'failed' } } });
  return counter.read;
}

/** The step instance statuses read by a `next` of a capped for_each over `items` items while both its leases run. */
async function cappedWork(items: number): Promise<number> {
  const { config, run } = await startFan(items, CAPPED);
  const running: string[] = [];
  for (const step of ['build[0]', 'build[1]']) {
    const next = await leaseNext(config, run, driver, { running });
    const lease = 'data' in next ? next.data.lease as { step: string; attempt: string } : undefined;
    expect(lease?.step).toBe(step);
    running.push(lease!.attempt);
  }
  counter.status = 0;
  expect(await leaseNext(config, run, driver, { running })).toMatchObject({ data: { wait: { reason: 'build[0] is leased' } } });
  return counter.status;
}

describe('settle cost', () => {
  it.each([
    ['an inline workflow', FAN, 'build[0].compile'],
    ['an inline workflow that runs another', NESTED_FAN, 'build[0].inner.compile'],
  ])('starts every item of an uncapped for_each over %s with work linear in the item count', async (_case, body, first) => {
    const small = await firstNextWork(100, body, first);
    const large = await firstNextWork(200, body, first);
    expect(small).toBeGreaterThan(100);
    // Linear work doubles with the items; quadratic work grows about four times.
    expect(large / small).toBeLessThan(3);
  });

  // The quadratic form runs several times longer, so this test has its own timeout and fails on its ratio, not on time.
  it('runs an uncapped for_each to the end with constant settle work per call', async () => {
    const small = await wholeRunWork(100);
    const large = await wholeRunWork(200);
    expect(small.parsed).toBeGreaterThan(10);
    expect(small.queue).toBeGreaterThan(1);
    // A report only makes its item's scopes dirty. Doubling unrelated items must not double each call's work.
    expect(large.parsed / small.parsed).toBeLessThan(1.5);
    expect(large.queue / small.queue).toBeLessThan(1.5);
  }, 60_000);

  it('does not scan unrelated persisted owners while reporting one fan-out', async () => {
    const small = await ownerLookupWork(SMALL_OWNER_COUNT);
    const large = await ownerLookupWork(LARGE_OWNER_COUNT);
    // Two serializations are required to checksum and save; reporting must not add another full registry scan.
    expect(small / SMALL_OWNER_COUNT).toBeLessThan(MAX_OWNER_REGISTRY_ACCESSES);
    expect(large / LARGE_OWNER_COUNT).toBeLessThan(MAX_OWNER_REGISTRY_ACCESSES);
  });

  it('drops persisted reconcile work when the run file was edited', async () => {
    const { config, run } = await startFan(20);
    const first = await leaseNext(config, run, driver);
    expect(first).toMatchObject({ data: { lease: { step: 'build[0].compile' } } });
    const file = runFilePath(config, run);
    const raw = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown>;
    expect(raw.reconcile).toBeDefined();
    raw.updatedAt = 'hand-edited';
    fs.writeFileSync(file, JSON.stringify(raw));

    const edited = await readRun(config, run);

    expect(edited).toEqual(expect.not.objectContaining({ reconcile: expect.anything() }));
  });

  it('drops signed reconcile work from a future version', async () => {
    const { config, run } = await startFan(2);
    await leaseNext(config, run, driver);
    const file = runFilePath(config, run);
    const raw = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown> & {
      reconcile: { version: number };
    };
    raw.reconcile.version = FUTURE_RECONCILE_VERSION;
    delete raw.reconcileChecksum;
    raw.reconcileChecksum = createHash(CHECKSUM_ALGORITHM).update(JSON.stringify(raw)).digest('hex');
    fs.writeFileSync(file, JSON.stringify(raw));

    const future = await readRun(config, run);

    expect(future).toEqual(expect.not.objectContaining({ reconcile: expect.anything() }));
  });

  it('normalizes signed runs with a future checksum version or malformed reconcile shape', async () => {
    const { config, run } = await startFan(2);
    const file = runFilePath(config, run);
    const signed = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown>;
    signed.reconcileChecksumVersion = FUTURE_CHECKSUM_VERSION;
    delete signed.reconcileChecksum;
    signed.reconcileChecksum = createHash(CHECKSUM_ALGORITHM).update(JSON.stringify(signed)).digest('hex');
    fs.writeFileSync(file, JSON.stringify(signed));
    expect(await readRun(config, run)).toEqual(expect.not.objectContaining({ reconcileChecksum: expect.anything() }));

    signed.reconcileChecksumVersion = RUN_RECONCILE_CHECKSUM_VERSION;
    signed.reconcile = { version: RUN_RECONCILE_VERSION, fanOuts: null };
    delete signed.reconcileChecksum;
    signed.reconcileChecksum = createHash(CHECKSUM_ALGORITHM).update(JSON.stringify(signed)).digest('hex');
    fs.writeFileSync(file, JSON.stringify(signed));

    expect(await readRun(config, run)).toEqual(expect.not.objectContaining({ reconcile: expect.anything() }));
  });

  it('canonicalizes a run before signing and writing it', async () => {
    const { config, run } = await startFan(2);
    const file = runFilePath(config, run);
    const loaded = await readRun(config, run);
    if (typeof loaded === 'string') throw new Error(`run was ${loaded}`);
    Object.assign(loaded.steps[0]!, { unexpected: true });

    await writeRun(config, loaded);

    const stored = JSON.parse(fs.readFileSync(file, 'utf8')) as { steps: Array<Record<string, unknown>> };

    expect(stored.steps[0]).not.toHaveProperty('unexpected');
  });

  it('falls back to full reconciliation when a persisted item queue is stale', async () => {
    const { config, run } = await startFan(2);
    const first = await leaseNext(config, run, driver);
    const lease = 'data' in first ? first.data.lease as { attempt: string } | undefined : undefined;
    expect(lease).toMatchObject({ attempt: expect.any(String) });
    const loaded = await readRun(config, run);
    if (typeof loaded === 'string') throw new Error(`run was ${loaded}`);
    loaded.reconcile!.fanOuts.build!.queue = [STALE_ITEM_ID];
    await writeRun(config, loaded);

    const next = await leaseNext(config, run, driver, { running: [lease!.attempt] });

    expect(next).toMatchObject({ data: { lease: { step: 'build[1].compile' } } });
  });

  it('drops a stale scope index before reading the scope in full', async () => {
    const { config, run } = await startFan(2);
    const loaded = await readRun(config, run);
    if (typeof loaded === 'string') throw new Error(`run was ${loaded}`);
    expect(indexedPosition(loaded, 'build')).toBe(0);
    loaded.reconcile = { version: RUN_RECONCILE_VERSION, fanOuts: {} };
    loaded.steps = loaded.steps.filter((step) => step.id !== 'after');

    expect(statesIn(loaded, topScope(loaded)).map((step) => step.id)).toEqual(['build']);
    expect(loaded.reconcile).toBeUndefined();
  });

  it('removes an already settled item left in a persisted queue', async () => {
    const { config, run } = await startFan(2);
    for (;;) {
      const next = await leaseNext(config, run, driver);
      const lease = 'data' in next ? next.data.lease as { attempt: string; revision: number; step: string } | undefined : undefined;
      expect(lease).toBeDefined();
      expect(await reportAttempt(config, run, { attempt: lease!.attempt, revision: lease!.revision, outcome: 'success', driver }))
        .toMatchObject({ data: { accepted: true } });
      const current = await readRun(config, run);
      if (typeof current === 'string') throw new Error(`run was ${current}`);
      if (current.steps.find((step) => step.id === FIRST_ITEM_ID)?.status === 'succeeded') break;
    }
    const loaded = await readRun(config, run);
    if (typeof loaded === 'string') throw new Error(`run was ${loaded}`);
    const progress = loaded.reconcile!.fanOuts.build!;
    progress.queue.unshift(FIRST_ITEM_ID);
    progress.queued[FIRST_ITEM_ID] = true;
    progress.remaining += 1;
    await writeRun(config, loaded);

    await leaseNext(config, run, driver);

    const reconciled = await readRun(config, run);
    if (typeof reconciled === 'string') throw new Error(`run was ${reconciled}`);
    expect(reconciled.reconcile?.fanOuts.build?.queued[FIRST_ITEM_ID]).toBeUndefined();
  });

  it('leases independent fan-outs whose ids name inherited object properties', async () => {
    const { config, run } = await startFan(2, PROTOTYPE_FANS);

    await expect(leaseNext(config, run, driver)).resolves.toMatchObject({
      data: { lease: { step: 'alpha[0]' } },
    });
  });

  it.each(['forward', 'reverse'] as const)('skips a %s chain of items behind a failed item with work linear in the item count', async (order) => {
    const small = await skippedChainWork(100, order);
    const large = await skippedChainWork(200, order);
    expect(small).toBeGreaterThan(100);
    // Each skip makes the next item skippable; scanning every item again after each skip makes that quadratic.
    expect(large / small).toBeLessThan(3);
  });

  it('waits on a full max_parallel with work linear in the item count', async () => {
    const small = await cappedWork(100);
    const large = await cappedWork(200);
    expect(small).toBeGreaterThan(100);
    // Counting the started items again for each waiting item makes that quadratic.
    expect(large / small).toBeLessThan(3);
  });
});
