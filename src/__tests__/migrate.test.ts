import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import { realpathSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import fse from 'fs-extra';
import YAML from 'yaml';

vi.mock('../utils/logger.js', () => ({
  log: {
    info: vi.fn(),
    success: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
    dim: vi.fn(),
  },
}));

import { planMigration, runMigration, maybeMigrate } from '../migrate.js';
import { projectDataHome } from '../utils/partition.js';

// ─── Real-git migration tests (issue #374 P1-3) ─────────────────────────────
//
// A legacy install kept machine data in `<repo>/.teamai/`, including a real git
// team-repo clone. Migration copies it into the partition, verifies, atomically
// renames, and retires the source to `.teamai.bak/`. These tests build a REAL
// legacy layout (real anchors + a real nested git clone) rather than an empty
// fixture, so the load-bearing details — anchor resolution, `.git` survival,
// atomicity — are genuinely exercised.

function git(cwd: string, ...args: string[]): void {
  execFileSync('git', args, { cwd, stdio: 'pipe' });
}

let base: string;
let repoRoot: string;
let homeDir: string;
let legacyDir: string;

/** Write a minimal but schema-valid legacy project config into legacyDir. */
async function writeLegacyConfig(overrides: Record<string, unknown> = {}): Promise<void> {
  const cfg = {
    repo: {
      localPath: path.join(legacyDir, 'team-repo'),
      remote: 'git@example.com:team/repo.git',
      kind: 'git',
    },
    username: 'tester',
    scope: 'project',
    ...overrides,
  };
  await fse.ensureDir(legacyDir);
  await fse.writeFile(path.join(legacyDir, 'config.yaml'), YAML.stringify(cfg));
}

/** Build a real, non-empty legacy `.teamai/` with a genuine git team-repo clone. */
async function seedLegacyLayout(): Promise<void> {
  await writeLegacyConfig();
  await fse.writeJson(path.join(legacyDir, 'state.json'), { lastSync: 'x' });
  await fse.writeFile(path.join(legacyDir, 'env'), 'TEAM_TOKEN=s3cret\n');
  await fse.writeJson(path.join(legacyDir, 'search-index.json'), { docs: [] });

  // A real git clone under team-repo/ — the `.git` dir is what the copyDir filter
  // would silently drop, so the test must assert it survives.
  const teamRepo = path.join(legacyDir, 'team-repo');
  await fse.ensureDir(teamRepo);
  git(teamRepo, 'init', '-q');
  git(teamRepo, 'config', 'user.email', 'test@example.com');
  git(teamRepo, 'config', 'user.name', 'Test');
  await fse.writeFile(path.join(teamRepo, 'README'), 'team\n');
  git(teamRepo, 'add', '.');
  git(teamRepo, 'commit', '-q', '-m', 'seed');

  // A per-worktree managed-mcp subtree (P1-2C layout).
  const wsDir = path.join(legacyDir, 'workspaces', 'abc123def456');
  await fse.ensureDir(wsDir);
  await fse.writeJson(path.join(wsDir, 'managed-mcp.json'), { 'claude:project': {} });
}

beforeEach(() => {
  base = realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'teamai-migrate-')));
  repoRoot = path.join(base, 'business-repo');
  fs.mkdirSync(repoRoot);
  git(repoRoot, 'init', '-q');
  git(repoRoot, 'config', 'user.email', 'test@example.com');
  git(repoRoot, 'config', 'user.name', 'Test');
  git(repoRoot, 'commit', '--allow-empty', '-q', '-m', 'init');

  homeDir = path.join(base, 'home');
  fs.mkdirSync(homeDir);
  legacyDir = path.join(repoRoot, '.teamai');

  vi.stubEnv('HOME', homeDir);
});

afterEach(() => {
  vi.unstubAllEnvs();
  try {
    fs.rmSync(base, { recursive: true, force: true });
  } catch {
    /* best effort */
  }
});

describe('planMigration', () => {
  it('plans a migration for a legacy git-mode project install', async () => {
    await seedLegacyLayout();
    const plan = await planMigration(repoRoot);
    expect(plan).not.toBeNull();
    expect(plan!.legacyDir).toBe(legacyDir);
    expect(plan!.partitionDir).toBe(projectDataHome(repoRoot));
    expect(plan!.anchor).toBe(repoRoot);
  });

  it('skips when no legacy config.yaml exists', async () => {
    expect(await planMigration(repoRoot)).toBeNull();
  });

  it('skips when a partition config already exists (partition is authoritative)', async () => {
    await seedLegacyLayout();
    const partition = projectDataHome(repoRoot);
    await fse.ensureDir(partition);
    await fse.writeFile(path.join(partition, 'config.yaml'), 'repo: {}\n');
    expect(await planMigration(repoRoot)).toBeNull();
  });

  it('skips a user-scope legacy config', async () => {
    await writeLegacyConfig({ scope: 'user' });
    expect(await planMigration(repoRoot)).toBeNull();
  });

  it('skips a self-mode legacy config (its .teamai is committed knowledge)', async () => {
    await writeLegacyConfig({ repo: { localPath: legacyDir, remote: '', kind: 'self' } });
    expect(await planMigration(repoRoot)).toBeNull();
  });

  it('skips outside a git repository', async () => {
    const plain = path.join(base, 'plain');
    fs.mkdirSync(plain);
    await fse.ensureDir(path.join(plain, '.teamai'));
    await fse.writeFile(
      path.join(plain, '.teamai', 'config.yaml'),
      YAML.stringify({ repo: { localPath: '', remote: '', kind: 'git' }, username: 'x', scope: 'project' }),
    );
    expect(await planMigration(plain)).toBeNull();
  });

  it('skips a malformed legacy config rather than throwing', async () => {
    await fse.ensureDir(legacyDir);
    await fse.writeFile(path.join(legacyDir, 'config.yaml'), ':::not yaml:::\n');
    expect(await planMigration(repoRoot)).toBeNull();
  });
});

describe('runMigration', () => {
  it('migrates into the partition, keeps the git clone intact, and retires the source', async () => {
    await seedLegacyLayout();
    const plan = await planMigration(repoRoot);
    const result = await runMigration(plan!);
    expect(result).toBe('migrated');

    const partition = projectDataHome(repoRoot);
    // Machine data landed in the partition.
    expect(await fse.pathExists(path.join(partition, 'config.yaml'))).toBe(true);
    expect(await fse.pathExists(path.join(partition, 'state.json'))).toBe(true);
    expect(await fse.pathExists(path.join(partition, 'search-index.json'))).toBe(true);
    expect(await fse.pathExists(path.join(partition, 'workspaces', 'abc123def456', 'managed-mcp.json'))).toBe(true);
    // The anchor reverse-lookup file was written.
    expect((await fse.readFile(path.join(partition, 'anchor'), 'utf-8')).trim()).toBe(repoRoot);

    // team-repo/.git SURVIVED — the clone is still a working repo (proves raw
    // fse.copy was used, not the .git-filtering copyDir).
    const migratedRepo = path.join(partition, 'team-repo');
    expect(await fse.pathExists(path.join(migratedRepo, '.git'))).toBe(true);
    expect(() => git(migratedRepo, 'status')).not.toThrow();
    expect(() => git(migratedRepo, 'rev-parse', 'HEAD')).not.toThrow();

    // Source retired to .bak, original gone → workspace zero-residue.
    expect(await fse.pathExists(legacyDir)).toBe(false);
    expect(await fse.pathExists(`${legacyDir}.bak`)).toBe(true);
    expect(await fse.pathExists(path.join(`${legacyDir}.bak`, 'config.yaml'))).toBe(true);
  });

  it('does not carry a live sync-lock into the backup', async () => {
    await seedLegacyLayout();
    const plan = await planMigration(repoRoot);
    await runMigration(plan!);
    // The lock lived in legacyDir and must be released before the .bak rename,
    // so neither the partition nor the backup keeps a stale lock.
    expect(await fse.pathExists(path.join(`${legacyDir}.bak`, '.sync-lock'))).toBe(false);
    expect(await fse.pathExists(path.join(projectDataHome(repoRoot), '.sync-lock'))).toBe(false);
  });

  it('does not copy disposable worktrees or lock files', async () => {
    await seedLegacyLayout();
    await fse.ensureDir(path.join(legacyDir, 'reports-wt'));
    await fse.writeFile(path.join(legacyDir, 'reports-wt', 'x'), 'stale\n');
    await fse.writeFile(path.join(legacyDir, '.update-lock'), '{}');
    const plan = await planMigration(repoRoot);
    await runMigration(plan!);
    const partition = projectDataHome(repoRoot);
    expect(await fse.pathExists(path.join(partition, 'reports-wt'))).toBe(false);
    expect(await fse.pathExists(path.join(partition, '.update-lock'))).toBe(false);
  });

  it('is idempotent: a second run stands down once the partition exists', async () => {
    await seedLegacyLayout();
    await runMigration((await planMigration(repoRoot))!);
    // Legacy is now .bak; planMigration returns null (nothing to migrate).
    expect(await planMigration(repoRoot)).toBeNull();
  });

  it('recovers from a leftover staging dir (interrupted prior run)', async () => {
    await seedLegacyLayout();
    const partition = projectDataHome(repoRoot);
    const staging = `${partition}.staging`;
    // Simulate a crash mid-copy: a partial staging dir is left behind.
    await fse.ensureDir(staging);
    await fse.writeFile(path.join(staging, 'garbage'), 'partial\n');
    const result = await runMigration((await planMigration(repoRoot))!);
    expect(result).toBe('migrated');
    // Stale staging content was discarded, not merged.
    expect(await fse.pathExists(path.join(partition, 'garbage'))).toBe(false);
    expect(await fse.pathExists(path.join(partition, 'config.yaml'))).toBe(true);
  });

  it('dry-run writes nothing', async () => {
    await seedLegacyLayout();
    const plan = await planMigration(repoRoot);
    const result = await runMigration(plan!, { dryRun: true });
    expect(result).toBe('dry-run');
    const partition = projectDataHome(repoRoot);
    expect(await fse.pathExists(partition)).toBe(false);
    // Source untouched.
    expect(await fse.pathExists(legacyDir)).toBe(true);
    expect(await fse.pathExists(`${legacyDir}.bak`)).toBe(false);
  });
});

describe('maybeMigrate', () => {
  it('is a no-op when there is nothing to migrate', async () => {
    // No legacy layout; must not throw.
    const spy = vi.spyOn(process, 'cwd').mockReturnValue(repoRoot);
    try {
      await expect(maybeMigrate()).resolves.toBeUndefined();
    } finally {
      spy.mockRestore();
    }
  });
});
