import { promises as fs } from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import { BanClient, BanClientOptions, BanFeature } from './lib/ban.js';
import { logger } from './lib/logger.js';

export interface InputRecord {
  nom: string;
  adresse: string;
  ville: string;
  postcode?: string;
}

export type GeocodeStatus = 'ok' | 'error';

export interface GeocodedEntry {
  input: InputRecord;
  feature: BanFeature | null;
  status: GeocodeStatus;
  reason?: string;
}

export interface GeocodeOptions {
  inputPath: string;
  cachePath: string;
  useCache: boolean;
  minScore: number;
  concurrency: number;
  onProgress?: (processed: number, total: number) => void;
}

export interface GeocodeSummary {
  geocoded: GeocodedEntry[];
  errors: GeocodedEntry[];
  stats: {
    total: number;
    errors: number;
    apiCalls: number;
    fromCache: number;
  };
}

interface ProcessedRow {
  entry: GeocodedEntry;
  fromCache: boolean;
  apiCall: boolean;
  isError: boolean;
}

function validateRecord(record: Record<string, string>): InputRecord | null {
  const nom = record.nom?.trim();
  const adresse = record.adresse?.trim();
  const ville = record.ville?.trim();
  const postcode = record.postcode?.trim() || record.cp?.trim();

  if (!nom || !adresse || !ville) {
    return null;
  }

  return { nom, adresse, ville, postcode };
}

export async function geocodeFile(options: GeocodeOptions): Promise<GeocodeSummary> {
  const { inputPath, cachePath, useCache, minScore, concurrency, onProgress } = options;
  const absoluteInput = path.resolve(inputPath);
  const fileContent = await fs.readFile(absoluteInput, 'utf8');

  const parsed = parse(fileContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  }) as Record<string, string>[];

  const banOptions: BanClientOptions = {
    cachePath,
    concurrency,
    useCache,
  };
  const client = new BanClient(banOptions);

  const total = parsed.length;
  let processedCount = 0;

  logger.info({ input: absoluteInput, total }, 'Starting geocoding batch');
  console.log(`[Geocode] Starting batch for ${absoluteInput} with ${total} rows`);

  const processRow = async (row: Record<string, string>): Promise<ProcessedRow> => {
    const input = validateRecord(row);
    if (!input) {
      logger.warn({ row }, 'Skipping row with missing mandatory fields');
      console.log('[Geocode] Skipping row due to missing mandatory fields', row);
      const entry: GeocodedEntry = {
        input: {
          nom: row.nom ?? '',
          adresse: row.adresse ?? '',
          ville: row.ville ?? '',
          postcode: row.postcode ?? row.cp,
        },
        feature: null,
        status: 'error',
        reason: 'missing_fields',
      };
      return { entry, fromCache: false, apiCall: false, isError: true };
    }

    try {
      logger.debug({ input }, 'Geocoding input row');
      console.log('[Geocode] Geocoding input', input);
      const { feature, fromCache: cached } = await client.geocode(input.adresse, input.ville, input.postcode);
      if (!feature) {
        const entry: GeocodedEntry = { input, feature: null, status: 'error', reason: 'no_result' };
        logger.info({ input }, 'No BAN result for row');
        console.log('[Geocode] No BAN result for', input);
        return { entry, fromCache: cached, apiCall: !cached, isError: true };
      }

      const entry: GeocodedEntry = { input, feature, status: 'ok' };
      logger.debug({ input, score: feature.properties.score }, 'Geocoding successful');
      console.log('[Geocode] Geocoding successful', { input, score: feature.properties.score });
      if (feature.properties.score < minScore) {
        logger.warn({ input, score: feature.properties.score }, 'BAN score below threshold but accepted');
        console.log('[Geocode] BAN score below threshold but accepted', { input, score: feature.properties.score });
      }
      return { entry, fromCache: cached, apiCall: !cached, isError: false };
    } catch (error) {
      logger.error({ err: error, input }, 'Geocoding failed');
      console.log('[Geocode] Geocoding failed', error);
      const entry: GeocodedEntry = { input, feature: null, status: 'error', reason: 'exception' };
      return { entry, fromCache: false, apiCall: false, isError: true };
    }
  };

  const promises = parsed.map(async (row) => {
    const result = await processRow(row);
    processedCount += 1;
    onProgress?.(processedCount, total);
    return result;
  });

  const results = await Promise.all(promises);

  const geocoded: GeocodedEntry[] = [];
  const errors: GeocodedEntry[] = [];

  let apiCalls = 0;
  let fromCacheCount = 0;

  for (const result of results) {
    geocoded.push(result.entry);
    if (result.isError) {
      errors.push(result.entry);
    }
    if (result.apiCall) {
      apiCalls += 1;
    }
    if (result.fromCache) {
      fromCacheCount += 1;
    }
  }

  logger.info(
    {
      total,
      errors: errors.length,
      apiCalls,
      fromCache: fromCacheCount,
    },
    'Finished geocoding batch',
  );
  console.log('[Geocode] Finished batch', {
    total,
    errors: errors.length,
    apiCalls,
    fromCache: fromCacheCount,
  });

  return {
    geocoded,
    errors,
    stats: {
      total,
      errors: errors.length,
      apiCalls,
      fromCache: fromCacheCount,
    },
  };
}
