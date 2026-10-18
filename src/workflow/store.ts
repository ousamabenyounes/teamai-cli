import { createHash, randomBytes } from 'node:crypto';
import path from 'node:path';
import { z } from 'zod';
import { readFileSafe, writeFileAtomic } from '../utils/fs.js';
import { getDataHome, type LocalConfig } from '../types.js';
import { ArchivedPassSchema, StepStateSchema } from './step-state.js';
import { indexReconcile } from './scope.js';
import { RUN_RECONCILE_CHECKSUM_VERSION, RUN_RECONCILE_VERSION, type RunState, type WorkflowDefinition } from './types.js';

/**
 * Run ids: `r` + 16 hex digits. Records are a team-wide key on the reports
 * branch (#1083), and 8 hex digits reach ~1% collision odds by 10,000 runs —
 * a collision would overwrite another member's record, so new runs get 16.
 * The legacy 8-hex form still resolves for runs created before the switch.
 */
const RUN_ID = /^r[0-9a-f]{8}(?:[0-9a-f]{8})?$/;
const RECONCILE_CHECKSUM_ALGORITHM = 'sha256';

/** The engine-owned checksum excludes itself, so it is stable across a write and its following read. */
function reconcileChecksum(run: RunState): string {
  const { reconcileChecksum: _stored, ...value } = run;
  return createHash(RECONCILE_CHECKSUM_ALGORITHM).update(JSON.stringify(value)).digest('hex');
}

export function isRunId(runId: string): boolean {
  return RUN_ID.test(runId);
}

/** A fresh run id: 16 hex digits (see RUN_ID for why not 8). */
export function generateRunId(): string {
  return `r${randomBytes(8).toString('hex')}`;
}

export function runFilePath(config: LocalConfig, runId: string): string {
  return path.join(getDataHome(config), 'workflows', 'runs', `${runId}.json`);
}

export function runLockPath(config: LocalConfig, runId: string): string {
  return path.join(getDataHome(config), 'workflows', 'runs', `${runId}.lock`);
}

function isDefinition(value: unknown): value is WorkflowDefinition {
  if (!value || typeof value !== 'object') return false;
  const definition = value as WorkflowDefinition;
  return definition.schemaVersion === 'v1alpha1'
    && typeof definition.name === 'string'
    && typeof definition.description === 'string'
    && Array.isArray(definition.steps);
}

export function isRunState(value: unknown): value is RunState {
  if (!value || typeof value !== 'object') return false;
  const run = value as RunState;
  return run.schemaVersion === 'v1alpha1'
    && isRunId(run.id)
    && typeof run.name === 'string'
    && typeof run.revision === 'number'
    && (run.status === 'running' || run.status === 'succeeded' || run.status === 'failed')
    && isDefinition(run.definition)
    && Array.isArray(run.steps)
    && Array.isArray(run.settledAttempts);
}

/** The parts of a run file the step union governs, with what a file written before them reads as. */
const RunProgressSchema = z.object({
  steps: z.array(StepStateSchema),
  cancelledAttempts: z.array(z.string()).default([]),
  archive: z.array(ArchivedPassSchema).default([]),
  round: z.number().int().optional(),
});

const ReconcileProgressSchema = z.object({
  version: z.literal(RUN_RECONCILE_VERSION),
  fanOuts: z.record(z.object({
    queue: z.array(z.string()),
    queued: z.record(z.literal(true)),
    remaining: z.number().int().nonnegative(),
  })),
});

function hasCurrentReconcileShape(run: RunState): boolean {
  if (run.reconcile === undefined) return true;
  if (!run.reconcile || typeof run.reconcile !== 'object') return false;
  return run.reconcile.version === RUN_RECONCILE_VERSION
    && !!run.reconcile.fanOuts && typeof run.reconcile.fanOuts === 'object' && !Array.isArray(run.reconcile.fanOuts);
}

export async function readRun(config: LocalConfig, runId: string): Promise<RunState | 'missing' | 'corrupt'> {
  const raw = await readFileSafe(runFilePath(config, runId));
  if (raw === null) return 'missing';
  try {
    const value: unknown = JSON.parse(raw);
    if (!isRunState(value)) return 'corrupt';
    const checksumValid = typeof value.reconcileChecksum === 'string' && value.reconcileChecksum === reconcileChecksum(value);
    if (checksumValid && value.reconcileChecksumVersion === RUN_RECONCILE_CHECKSUM_VERSION && hasCurrentReconcileShape(value)) {
      if (value.reconcile) indexReconcile(value);
      return value;
    }
    const progress = RunProgressSchema.safeParse(value);
    if (!progress.success) return 'corrupt';
    const { reconcile: _stale, reconcileChecksum: _checksum, reconcileChecksumVersion: _checksumVersion, ...rest } = value;
    const normalized: RunState = { ...rest, ...progress.data };
    return normalized;
  } catch {
    return 'corrupt';
  }
}

/** Canonicalizes an externally supplied run before persisting engine-owned metadata. */
export async function writeRun(config: LocalConfig, run: RunState): Promise<void> {
  const progress = RunProgressSchema.parse(run);
  const reconcile = run.reconcile === undefined ? undefined : ReconcileProgressSchema.parse(run.reconcile);
  const { reconcile: _rawReconcile, reconcileChecksum: _checksum, reconcileChecksumVersion: _checksumVersion, ...rest } = run;
  await writeEngineRun(config, { ...rest, ...progress, ...(reconcile ? { reconcile } : {}) });
}

/** Writes a run produced by `readRun` and mutated only through the engine's typed transitions. */
export async function writeEngineRun(config: LocalConfig, run: RunState): Promise<void> {
  run.cancelledAttempts ??= [];
  run.archive ??= [];
  run.reconcileChecksumVersion = RUN_RECONCILE_CHECKSUM_VERSION;
  run.reconcileChecksum = reconcileChecksum(run);
  await writeFileAtomic(runFilePath(config, run.id), `${JSON.stringify(run)}\n`);
}
