import { createReadStream, promises as fs } from 'fs';
import path from 'path';
import readline from 'readline';
import { writeCsv } from '../lib/csv.js';
import { logger } from '../lib/logger.js';
import { normalizeOsmElement } from './normalize.js';
import {
  createOverpassLimiter,
  findAdminArea,
  buildVenueQuery,
  fetchOverpass,
  OSM_AMENITIES,
} from './overpass.js';
import { NormalizedOsmVenue, OsmEntry } from './types.js';

export interface OsmFetchOptions {
  city: string;
  country?: string;
  adminLevel: number;
  outDir: string;
  useCache: boolean;
  concurrency: number;
  onProgress?: (processed: number, total: number) => void;
}

export interface OsmFetchStats {
  total: number;
  kept: number;
  ambiguous: number;
  fromCache: number;
  apiCalls: number;
  areaAmbiguous: boolean;
}

export interface OsmFetchResult {
  entries: OsmEntry[];
  stats: OsmFetchStats;
}

interface AmbiguousRecord extends Record<string, unknown> {
  osm_type?: string;
  osm_id?: number;
  name?: string;
  reason: string;
  details?: string;
}

async function readCacheEntries(cachePath: string): Promise<OsmEntry[]> {
  const entries: OsmEntry[] = [];
  const fileStream = createReadStream(cachePath, 'utf8');
  const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line.trim()) {
      continue;
    }
    try {
      const parsed = JSON.parse(line) as OsmEntry;
      entries.push(parsed);
    } catch (error) {
      logger.warn({ err: error, line }, 'Failed to parse cached OSM entry');
    }
  }

  return entries;
}

function createCacheWriter(filePath: string): (entry: OsmEntry) => Promise<void> {
  let writeChain = Promise.resolve();
  return async (entry: OsmEntry) => {
    writeChain = writeChain.then(() => fs.appendFile(filePath, `${JSON.stringify(entry)}\n`, 'utf8'));
    await writeChain;
  };
}

export async function fetchOsmVenues(options: OsmFetchOptions): Promise<OsmFetchResult> {
  const cachePath = path.join(options.outDir, 'osm_fetched.jsonl');
  const ambiguousPath = path.join(options.outDir, 'ambiguous.csv');

  if (options.useCache) {
    try {
      await fs.access(cachePath);
      const cached = await readCacheEntries(cachePath);
      const ambiguousFromCache: AmbiguousRecord[] = cached
        .filter((entry) => entry.status === 'ambiguous')
        .map((entry) => ({
          osm_type: entry.raw?.type,
          osm_id: entry.raw?.id,
          name: entry.raw?.tags?.name ?? entry.raw?.tags?.operator ?? entry.raw?.tags?.brand,
          reason: entry.reason ?? 'ambiguous',
        }));

      if (ambiguousFromCache.length > 0) {
        await writeCsv(ambiguousPath, ambiguousFromCache, ['osm_type', 'osm_id', 'name', 'reason', 'details']);
      } else {
        await fs.rm(ambiguousPath, { force: true });
      }

      return {
        entries: cached,
        stats: {
          total: cached.length,
          kept: cached.filter((entry) => entry.status === 'ok').length,
          ambiguous: cached.filter((entry) => entry.status === 'ambiguous').length,
          fromCache: cached.length,
          apiCalls: 0,
          areaAmbiguous: false,
        },
      };
    } catch (error) {
      logger.warn({ err: error }, 'Cache file not available, fetching from Overpass');
    }
  }

  await fs.rm(cachePath, { force: true });

  const ambiguousRecords: AmbiguousRecord[] = [];
  const limiter = createOverpassLimiter(options.concurrency);
  const writeCacheEntry = createCacheWriter(cachePath);

  const areaResult = await findAdminArea({
    city: options.city,
    country: options.country,
    adminLevel: options.adminLevel,
  });

  if (!areaResult.selected || !areaResult.areaId) {
    ambiguousRecords.push({
      reason: 'area_not_found',
      details: `No administrative area found for ${options.city}`,
    });
    await writeCsv(ambiguousPath, ambiguousRecords, ['reason', 'details']);
    return {
      entries: [],
      stats: {
        total: 0,
        kept: 0,
        ambiguous: ambiguousRecords.length,
        fromCache: 0,
        apiCalls: 1,
        areaAmbiguous: false,
      },
    };
  }

  if (areaResult.ambiguous) {
    ambiguousRecords.push({
      reason: 'area_ambiguous',
      details: `Multiple administrative areas matched for ${options.city}: ${areaResult.candidates.map((c) => c.name).join(', ')}`,
    });
  }

  logger.info({ areaId: areaResult.areaId, adminLevel: options.adminLevel }, 'Resolved OSM area');

  const entries: OsmEntry[] = [];
  const seen = new Set<string>();
  let apiCalls = 1;

  const tasks = OSM_AMENITIES.map((amenity) => limiter(async () => {
    const query = buildVenueQuery(areaResult.areaId ?? 0, amenity);
    const response = await fetchOverpass(query);
    apiCalls += 1;

    for (const element of response.elements) {
      const key = `${element.type}:${element.id}`;
      if (seen.has(key)) {
        continue;
      }
      seen.add(key);

      const normalized = normalizeOsmElement(element, { defaultCity: options.city });
      if (!normalized) {
        const name = element.tags?.name ?? element.tags?.operator ?? element.tags?.brand;
        const reason = !name ? 'missing_name' : 'missing_coordinates';
        const entry: OsmEntry = { status: 'ambiguous', reason, raw: element };
        entries.push(entry);
        ambiguousRecords.push({
          osm_type: element.type,
          osm_id: element.id,
          name: name ?? undefined,
          reason,
        });
        await writeCacheEntry(entry);
        continue;
      }

      const entry: OsmEntry = { status: 'ok', venue: normalized };
      entries.push(entry);
      await writeCacheEntry(entry);
    }
  }));

  let processedAmenities = 0;
  for (const task of tasks) {
    await task;
    processedAmenities += 1;
    options.onProgress?.(processedAmenities, OSM_AMENITIES.length);
  }

  if (ambiguousRecords.length > 0) {
    await writeCsv(ambiguousPath, ambiguousRecords, ['osm_type', 'osm_id', 'name', 'reason', 'details']);
  } else {
    await fs.rm(ambiguousPath, { force: true });
  }

  const kept = entries.filter((entry) => entry.status === 'ok').length;
  const ambiguous = ambiguousRecords.length;

  return {
    entries,
    stats: {
      total: entries.length,
      kept,
      ambiguous,
      fromCache: 0,
      apiCalls,
      areaAmbiguous: areaResult.ambiguous,
    },
  };
}

export function toVenuePayload(entry: NormalizedOsmVenue): {
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  tags?: string[];
  osm_type: string;
  osm_id: number;
  osm_url: string;
  osm_tags_raw: Record<string, string | undefined>;
} {
  return {
    nom: entry.name,
    adresse: entry.adresse,
    city: entry.city,
    latitude: entry.latitude,
    longitude: entry.longitude,
    tags: entry.type ? [entry.type] : undefined,
    osm_type: entry.osm_type,
    osm_id: entry.osm_id,
    osm_url: entry.osm_url,
    osm_tags_raw: entry.osm_tags_raw,
  };
}
