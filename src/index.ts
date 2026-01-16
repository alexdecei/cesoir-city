import { promises as fs } from 'fs';
import path from 'path';
import { Command } from 'commander';
import * as dotenv from 'dotenv';
import cliProgress from 'cli-progress';
import { geocodeFile } from './geocode.js';
import { logger } from './lib/logger.js';
import { processUpserts } from './upsert.js';
import { fetchOsmVenues } from './osm/fetch.js';
import { processOsmUpserts } from './osm/upsert.js';
import { runHomogenize } from './osm/homogenize.js';

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

  const reportPath = createReportPath(outDir, 'report.json');
  const report = {
    mode: 'geocode',
    input,
    total: summary.stats.total,
    errors: summary.stats.errors,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
    inserted: 0,
    updated: 0,
    conflicts: 0,
    duplicates: 0,
    dryRun: true,
    timestamp: new Date().toISOString(),
  };
  await writeReport(reportPath, report);

  logger.info({
    input,
    total: summary.stats.total,
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

  const upsertCsvPath = createReportPath(outDir, 'upserts.csv');
  const conflictCsvPath = createReportPath(outDir, 'conflicts.csv');
  const duplicatesCsvPath = createReportPath(outDir, 'duplicates.csv');

  const upsertReport = await processUpserts(summary.geocoded, {
    dryRun: options.dryRun ?? false,
    upsertCsvPath,
    conflictCsvPath,
    duplicateCsvPath: duplicatesCsvPath,
  });

  const reportPath = createReportPath(outDir, 'report.json');
  const report = {
    mode: 'run',
    input,
    total: summary.stats.total,
    errors: summary.stats.errors,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
    inserted: upsertReport.inserted,
    updated: upsertReport.updated,
    conflicts: upsertReport.conflicts,
    duplicates: upsertReport.duplicates,
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
    errors: summary.stats.errors,
    duplicates: upsertReport.duplicates,
  }, 'Upsert completed');
}

async function runOsmFetchCommand(options: {
  city: string;
  country?: string;
  adminLevel?: string;
  out?: string;
  continue?: boolean;
  concurrency?: string;
}): Promise<void> {
  const outDir = path.resolve(options.out ?? DEFAULT_OUTPUT_DIR);
  await ensureDirectory(outDir);

  const adminLevel = getNumber(options.adminLevel, 8);
  const concurrency = getNumber(options.concurrency, getNumber(process.env.OVERPASS_CONCURRENCY, 1));

  logger.info(
    {
      city: options.city,
      country: options.country,
      adminLevel,
      outDir,
      concurrency,
      reuseCache: options.continue ?? false,
    },
    'Running OSM fetch command',
  );

  const progressBar = new cliProgress.SingleBar({ clearOnComplete: true }, cliProgress.Presets.shades_classic);
  let progressStarted = false;

  const summary = await fetchOsmVenues({
    city: options.city,
    country: options.country,
    adminLevel,
    outDir,
    useCache: options.continue ?? false,
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

  const reportPath = createReportPath(outDir, 'report.json');
  const report = {
    mode: 'osm:fetch',
    city: options.city,
    country: options.country,
    adminLevel,
    total: summary.stats.total,
    kept: summary.stats.kept,
    ambiguous: summary.stats.ambiguous,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
    areaAmbiguous: summary.stats.areaAmbiguous,
    exportPath: summary.exportPath,
    inserted: 0,
    updated: 0,
    conflicts: 0,
    errors: 0,
    dryRun: true,
    timestamp: new Date().toISOString(),
  };
  await writeReport(reportPath, report);

  logger.info({
    city: options.city,
    total: summary.stats.total,
    kept: summary.stats.kept,
    ambiguous: summary.stats.ambiguous,
    apiCalls: summary.stats.apiCalls,
    exportPath: summary.exportPath,
  }, 'OSM fetch completed');
}

async function runOsmUpsertCommand(options: {
  city: string;
  country?: string;
  adminLevel?: string;
  out?: string;
  continue?: boolean;
  concurrency?: string;
  dryRun?: boolean;
}): Promise<void> {
  const outDir = path.resolve(options.out ?? DEFAULT_OUTPUT_DIR);
  await ensureDirectory(outDir);

  const adminLevel = getNumber(options.adminLevel, 8);
  const concurrency = getNumber(options.concurrency, getNumber(process.env.OVERPASS_CONCURRENCY, 1));

  logger.info(
    {
      city: options.city,
      country: options.country,
      adminLevel,
      outDir,
      concurrency,
      reuseCache: options.continue ?? false,
      dryRun: options.dryRun ?? false,
    },
    'Running OSM upsert command',
  );

  const progressBar = new cliProgress.SingleBar({ clearOnComplete: true }, cliProgress.Presets.shades_classic);
  let progressStarted = false;

  const summary = await fetchOsmVenues({
    city: options.city,
    country: options.country,
    adminLevel,
    outDir,
    useCache: options.continue ?? false,
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

  const upsertCsvPath = createReportPath(outDir, 'upserts.csv');
  const conflictCsvPath = createReportPath(outDir, 'conflicts.csv');

  const nowIso = new Date().toISOString();
  const upsertReport = await processOsmUpserts(summary.entries, {
    dryRun: options.dryRun ?? false,
    upsertCsvPath,
    conflictCsvPath,
    nowIso,
  });

  const reportPath = createReportPath(outDir, 'report.json');
  const report = {
    mode: 'osm:run',
    city: options.city,
    country: options.country,
    adminLevel,
    total: summary.stats.total,
    kept: summary.stats.kept,
    ambiguous: summary.stats.ambiguous,
    apiCalls: summary.stats.apiCalls,
    fromCache: summary.stats.fromCache,
    areaAmbiguous: summary.stats.areaAmbiguous,
    exportPath: summary.exportPath,
    inserted: upsertReport.inserted,
    updated: upsertReport.updated,
    conflicts: upsertReport.conflicts,
    errors: upsertReport.errors,
    dryRun: options.dryRun ?? false,
    timestamp: nowIso,
  };
  await writeReport(reportPath, report);

  logger.info({
    city: options.city,
    dryRun: options.dryRun ?? false,
    total: summary.stats.total,
    inserted: upsertReport.inserted,
    updated: upsertReport.updated,
    conflicts: upsertReport.conflicts,
    errors: upsertReport.errors,
  }, 'OSM upsert completed');
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
  .action(async (input: string, opts: { continue?: boolean; out?: string; scoreMin?: string; concurrency?: string }) => {
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
  .action(async (input: string, opts: { dryRun?: boolean; continue?: boolean; out?: string; scoreMin?: string; concurrency?: string }) => {
    try {
      await runUpsertCommand(input, opts);
    } catch (error) {
      logger.error({ err: error }, 'Run command failed');
      process.exitCode = 1;
    }
  });

program
  .command('osm:fetch')
  .requiredOption('--city <name>', 'City name to fetch from OSM')
  .option('--country <code>', 'Country ISO3166-1 code', 'FR')
  .option('--admin-level <level>', 'Administrative level (default 8)')
  .option('--continue', 'Reuse local OSM cache', false)
  .option('--out <dir>', 'Output directory', DEFAULT_OUTPUT_DIR)
  .option('--concurrency <value>', 'Maximum concurrent Overpass requests (default from env)')
  .action(async (opts: { city: string; country?: string; adminLevel?: string; out?: string; continue?: boolean; concurrency?: string }) => {
    try {
      await runOsmFetchCommand(opts);
    } catch (error) {
      logger.error({ err: error }, 'OSM fetch command failed');
      process.exitCode = 1;
    }
  });

program
  .command('osm:run')
  .requiredOption('--city <name>', 'City name to fetch from OSM')
  .option('--country <code>', 'Country ISO3166-1 code', 'FR')
  .option('--admin-level <level>', 'Administrative level (default 8)')
  .option('--dry-run', 'Skip database writes', false)
  .option('--continue', 'Reuse local OSM cache', false)
  .option('--out <dir>', 'Output directory', DEFAULT_OUTPUT_DIR)
  .option('--concurrency <value>', 'Maximum concurrent Overpass requests (default from env)')
  .action(async (opts: {
    city: string;
    country?: string;
    adminLevel?: string;
    dryRun?: boolean;
    out?: string;
    continue?: boolean;
    concurrency?: string;
  }) => {
    try {
      await runOsmUpsertCommand(opts);
    } catch (error) {
      logger.error({ err: error }, 'OSM run command failed');
      process.exitCode = 1;
    }
  });

program
  .command('osm:homogenize')
  .option('--city <name>', 'City name to homogenize', 'Nantes')
  .requiredOption('--in <path>', 'Path to OSM export file (db-shaped or OSM-normalized)')
  .option('--out <dir>', 'Output directory', DEFAULT_OUTPUT_DIR)
  .option('--distance-threshold-m <value>', 'Distance threshold in meters (default 500)')
  .option('--dry-run', 'No-op flag (homogenize never mutates DB)', true)
  .action(async (opts: {
    city: string;
    in: string;
    out?: string;
    distanceThresholdM?: string;
  }) => {
    try {
      const distanceThresholdM = getNumber(opts.distanceThresholdM, 500);
      await runHomogenize({
        city: opts.city,
        inputPath: opts.in,
        outDir: path.resolve(opts.out ?? DEFAULT_OUTPUT_DIR),
        distanceThresholdM,
      });
    } catch (error) {
      logger.error({ err: error }, 'OSM homogenize command failed');
      process.exitCode = 1;
    }
  });

program.parseAsync(process.argv);
