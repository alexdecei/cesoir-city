import { createReadStream, promises as fs } from 'fs';
import path from 'path';
import readline from 'readline';
import { writeCsv } from '../lib/csv.js';
import { logger } from '../lib/logger.js';
import { writeDbExport } from '../export/dbShape.js';
import { normalizeOsmElement } from './normalize.js';
import {
  createOverpassLimiter,
  buildVenueQuery,
  fetchOverpass,
  OSM_AMENITIES,
} from './overpass.js';
import { findAdminArea, writeAmbiguousAreas } from './area.js';
import { NormalizedOsmVenue, OsmEntry } from './types.js';
import { VenuePayload } from '../lib/supabase.js';
import { stripEmpty } from '../export/dbShape.js';

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
  selectedAreaId?: number;
  selectedRelationId?: number;
  selectedAreaName?: string;
  parisSpecialCase: boolean;
}

export interface OsmFetchResult {
  entries: OsmEntry[];
  stats: OsmFetchStats;
  exportPath: string;
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
  const areaResult = await findAdminArea({
    city: options.city,
    country: options.country,
    adminLevel: options.adminLevel,
  });

  if (areaResult.parisSpecialCase) {
    logger.info({
      relationId: areaResult.relationId,
      areaId: areaResult.areaId,
      reason: 'Grand Paris perimeter instead of intramuros',
    }, 'Paris special-case enabled');
  }

  const cacheKey = areaResult.areaId ? `osm_cache_${areaResult.areaId}.jsonl` : 'osm_cache.jsonl';
  const cachePath = path.join(options.outDir, cacheKey);
  const ambiguousPath = path.join(options.outDir, 'ambiguous.csv');
  const ambiguousAreasPath = path.join(options.outDir, 'ambiguous_areas.csv');
  const exportPath = path.join(options.outDir, 'venues.db.jsonl');
  const nowIso = new Date().toISOString();

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

      const cachedVenues = cached
        .filter((entry) => entry.status === 'ok' && entry.venue)
        .map((entry) => entry.venue as NormalizedOsmVenue);

      await writeDbExport(cachedVenues, exportPath, nowIso);

      return {
        entries: cached,
        stats: {
          total: cached.length,
          kept: cached.filter((entry) => entry.status === 'ok').length,
          ambiguous: cached.filter((entry) => entry.status === 'ambiguous').length,
          fromCache: cached.length,
          apiCalls: 0,
          areaAmbiguous: areaResult.ambiguous,
          selectedAreaId: areaResult.areaId,
          selectedRelationId: areaResult.relationId,
          selectedAreaName: areaResult.areaName,
          parisSpecialCase: areaResult.parisSpecialCase,
        },
        exportPath,
      };
    } catch (error) {
      logger.warn({ err: error }, 'Cache file not available, fetching from Overpass');
    }
  }

  await fs.rm(cachePath, { force: true });

  const ambiguousRecords: AmbiguousRecord[] = [];
  const limiter = createOverpassLimiter(options.concurrency);
  const writeCacheEntry = createCacheWriter(cachePath);

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
        selectedAreaId: areaResult.areaId,
        selectedRelationId: areaResult.relationId,
        selectedAreaName: areaResult.areaName,
        parisSpecialCase: areaResult.parisSpecialCase,
      },
      exportPath,
    };
  }

  if (areaResult.ambiguous) {
    ambiguousRecords.push({
      reason: 'area_ambiguous',
      details: `Multiple administrative areas matched for ${options.city}: ${areaResult.candidates.map((c) => c.name).join(', ')}`,
    });
    await writeAmbiguousAreas(ambiguousAreasPath, areaResult.candidates);
  } else {
    await fs.rm(ambiguousAreasPath, { force: true });
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

  const venues = entries
    .filter((entry) => entry.status === 'ok' && entry.venue)
    .map((entry) => entry.venue as NormalizedOsmVenue);

  await writeDbExport(venues, exportPath, nowIso);

  return {
    entries,
    stats: {
      total: entries.length,
      kept,
      ambiguous,
      fromCache: 0,
      apiCalls,
      areaAmbiguous: areaResult.ambiguous,
      selectedAreaId: areaResult.areaId,
      selectedRelationId: areaResult.relationId,
      selectedAreaName: areaResult.areaName,
      parisSpecialCase: areaResult.parisSpecialCase,
    },
    exportPath,
  };
}

export function toVenuePayload(entry: NormalizedOsmVenue, nowIso: string): VenuePayload {
  const address = stripEmpty({ ...entry.address });
  const contact = stripEmpty({ ...entry.contact });
  const parsedCapacity = entry.capacity ? Number.parseInt(entry.capacity, 10) : undefined;
  const capacity = Number.isFinite(parsedCapacity) ? parsedCapacity : undefined;
  const stripTrailingSlashes = (value?: string): string | undefined => (value ? value.replace(/[\\/]+$/g, '') : undefined);

  return stripEmpty({
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
    address: Object.keys(address).length > 0 ? address : undefined,
    contact: Object.keys(contact).length > 0 ? contact : undefined,
    osm_venue_type: entry.type,
    opening_hours: entry.opening_hours ?? undefined,
    capacity,
    live_music: entry.live_music === 'yes' ? true : entry.live_music === 'no' ? false : undefined,
    website: stripTrailingSlashes(entry.contact.website ?? undefined),
    phone: entry.contact.phone ?? undefined,
    instagram: stripTrailingSlashes(entry.contact.instagram ?? undefined),
    facebook: stripTrailingSlashes(entry.contact.facebook ?? undefined),
    source: 'osm_seed',
    osm_last_sync_at: nowIso,
  });
}
