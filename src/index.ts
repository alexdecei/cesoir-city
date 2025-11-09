import { promises as fs } from 'fs';
import path from 'path';
import { Command } from 'commander';
import * as dotenv from 'dotenv';
import cliProgress from 'cli-progress';
import { geocodeFile, GeocodedEntry } from './geocode.js';
import { writeCsv } from './lib/csv.js';
import { logger } from './lib/logger.js';
import { roundCoord } from './lib/normalize.js';
import { processUpserts } from './upsert.js';

dotenv.config();

const DEFAULT_OUTPUT_DIR = 'out';

function getNumber(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

async function ensureDirectory(dirPath: string): Promise<void> {
  await fs.mkdir(dirPath, { recursive: true });
}

function createReportPath(outDir: string, fileName: string): string {
  return path.join(outDir, fileName);
}

async function writeReport(filePath: string, data: unknown): Promise<void> {
  await fs.writeFile(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function buildAmbiguousRecords(entries: GeocodedEntry[]): Record<string, unknown>[] {
  return entries.map((entry) => ({
    nom: entry.input.nom,
    adresse_input: entry.input.adresse,
    ville_input: entry.input.ville,
    postcode_input: entry.input.postcode ?? '',
    status: entry.status,
    reason: entry.reason ?? '',
    score: entry.feature?.properties.score ?? '',
    label: entry.feature?.properties.label ?? '',
    latitude: entry.feature ? roundCoord(entry.feature.geometry.coordinates[1]) : '',
    longitude: entry.feature ? roundCoord(entry.feature.geometry.coordinates[0]) : '',
  }));
}

async function runGeocodeCommand(input: string, options: { continue: boolean; out?: string; scoreMin?: string; concurrency?: string }): Promise<void> {
  const outDir = path.resolve(options.out ?? DEFAULT_OUTPUT_DIR);
  await ensureDirectory(outDir);

  const cachePath = path.join(outDir, 'geocoded.jsonl');

  if (!options.continue) {
    await fs.rm(cachePath, { force: true });
  }

  const concurrency = getNumber(options.concurrency, getNumber(process.env.GEOCODE_CONCURRENCY, 5));
  const minScore = getNumber(options.scoreMin, getNumber(process.env.GEOCODE_SCORE_MIN, 0.8));

  logger.info(
    {
      input,
      outDir,
      cachePath,
      concurrency,
      minScore,
      reuseCache: options.continue,
    },
    'Running geocode command',
  );

  const progressBar = new cliProgress.SingleBar({ clearOnComplete: true }, cliProgress.Presets.shades_classic);
  let progressStarted = false;

  const summary = await geocodeFile({
    inputPath: input,
    cachePath,
    useCache: options.continue,
    minScore,
    concurrency,
    onProgress: (processed, total) => {
      if (!progressStarted) {
        progressBar.start(total, processed);
        progressStarted = true;
      } else {
        progressBar.update(processed);
      }
    },
  });

  if (progressStarted) {
    progressBar.stop();
  }

  const ambiguousRecords = buildAmbiguousRecords([...summary.ambiguous, ...summary.errors]);
  const ambiguousPath = createReportPath(outDir, 'ambiguous.csv');
  await writeCsv(ambiguousPath, ambiguousRecords, [
    'nom',
    'adresse_input',
    'ville_input',
    'postcode_input',
    'status',
    'reason',
    'score',
    'label',
    'latitude',
    'longitude',
  ]);

  const reportPath = createReportPath(outDir, 'report.json');
  const report = {
    mode: 'geocode',
    input,
    total: summary.stats.total,
    ambiguous: summary.stats.ambiguous,
    errors: summary.stats.errors,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
    inserted: 0,
    updated: 0,
    conflicts: 0,
    dryRun: true,
    timestamp: new Date().toISOString(),
  };
  await writeReport(reportPath, report);

  logger.info({
    input,
    total: summary.stats.total,
    ambiguous: summary.stats.ambiguous,
    errors: summary.stats.errors,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
  }, 'Geocode completed');
}

async function runUpsertCommand(input: string, options: { dryRun?: boolean; continue?: boolean; out?: string; scoreMin?: string; concurrency?: string }): Promise<void> {
  const outDir = path.resolve(options.out ?? DEFAULT_OUTPUT_DIR);
  await ensureDirectory(outDir);

  const cachePath = path.join(outDir, 'geocoded.jsonl');

  if (!options.continue) {
    await fs.rm(cachePath, { force: true });
  }

  const concurrency = getNumber(options.concurrency, getNumber(process.env.GEOCODE_CONCURRENCY, 5));
  const minScore = getNumber(options.scoreMin, getNumber(process.env.GEOCODE_SCORE_MIN, 0.8));

  logger.info(
    {
      input,
      outDir,
      cachePath,
      concurrency,
      minScore,
      reuseCache: options.continue ?? false,
      dryRun: options.dryRun ?? false,
    },
    'Running upsert command',
  );

  const progressBar = new cliProgress.SingleBar({ clearOnComplete: true }, cliProgress.Presets.shades_classic);
  let progressStarted = false;

  const summary = await geocodeFile({
    inputPath: input,
    cachePath,
    useCache: options.continue ?? false,
    minScore,
    concurrency,
    onProgress: (processed, total) => {
      if (!progressStarted) {
        progressBar.start(total, processed);
        progressStarted = true;
      } else {
        progressBar.update(processed);
      }
    },
  });

  if (progressStarted) {
    progressBar.stop();
  }

  const ambiguousRecords = buildAmbiguousRecords([...summary.ambiguous, ...summary.errors]);
  const ambiguousPath = createReportPath(outDir, 'ambiguous.csv');
  await writeCsv(ambiguousPath, ambiguousRecords, [
    'nom',
    'adresse_input',
    'ville_input',
    'postcode_input',
    'status',
    'reason',
    'score',
    'label',
    'latitude',
    'longitude',
  ]);

  const upsertCsvPath = createReportPath(outDir, 'upserts.csv');
  const conflictCsvPath = createReportPath(outDir, 'conflicts.csv');

  const upsertReport = await processUpserts(summary.geocoded, {
    dryRun: options.dryRun ?? false,
    upsertCsvPath,
    conflictCsvPath,
  });

  const reportPath = createReportPath(outDir, 'report.json');
  const report = {
    mode: 'run',
    input,
    total: summary.stats.total,
    ambiguous: summary.stats.ambiguous,
    errors: summary.stats.errors,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
    inserted: upsertReport.inserted,
    updated: upsertReport.updated,
    conflicts: upsertReport.conflicts,
    errorsDuringUpsert: upsertReport.errors,
    dryRun: options.dryRun ?? false,
    timestamp: new Date().toISOString(),
  };
  await writeReport(reportPath, report);

  logger.info({
    input,
    dryRun: options.dryRun ?? false,
    total: summary.stats.total,
    inserted: upsertReport.inserted,
    updated: upsertReport.updated,
    conflicts: upsertReport.conflicts,
    ambiguous: summary.stats.ambiguous,
    errors: summary.stats.errors,
  }, 'Upsert completed');
}

const program = new Command();
program
  .name('venues-tool')
  .description('Geocode and upsert venues into Supabase');

program
  .command('geocode')
  .argument('<input>', 'Input CSV file')
  .option('--continue', 'Reuse local geocode cache', false)
  .option('--out <dir>', 'Output directory', DEFAULT_OUTPUT_DIR)
  .option('--score-min <value>', 'Minimum BAN score to accept (default from env)')
  .option('--concurrency <value>', 'Maximum concurrent BAN requests (default from env)')
  .option('--dry-run', 'No-op flag (geocode mode is always dry-run)')
  .action(async (input, opts) => {
    try {
      await runGeocodeCommand(input, opts);
    } catch (error) {
      logger.error({ err: error }, 'Geocode command failed');
      process.exitCode = 1;
    }
  });

program
  .command('run')
  .argument('<input>', 'Input CSV file')
  .option('--dry-run', 'Skip database writes', false)
  .option('--continue', 'Reuse local geocode cache', false)
  .option('--out <dir>', 'Output directory', DEFAULT_OUTPUT_DIR)
  .option('--score-min <value>', 'Minimum BAN score to accept (default from env)')
  .option('--concurrency <value>', 'Maximum concurrent BAN requests (default from env)')
  .action(async (input, opts) => {
    try {
      await runUpsertCommand(input, opts);
    } catch (error) {
      logger.error({ err: error }, 'Run command failed');
      process.exitCode = 1;
    }
  });

program.parseAsync(process.argv);
