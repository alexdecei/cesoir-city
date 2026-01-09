import pLimit from 'p-limit';
import { logger } from '../lib/logger.js';
import { AreaCandidate, OverpassResponse } from './types.js';

const DEFAULT_ENDPOINT = process.env.OVERPASS_URL ?? 'https://overpass-api.de/api/interpreter';
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_CONCURRENCY = Number(process.env.OVERPASS_CONCURRENCY ?? 1);

export const OSM_AMENITIES = [
  'bar',
  'pub',
  'nightclub',
  'theatre',
  'arts_centre',
  'casino',
  'community_centre',
  'concert_hall',
  'events_venue',
] as const;

export interface OverpassQueryOptions {
  endpoint?: string;
  timeoutMs?: number;
  maxRetries?: number;
}

export interface AreaSearchOptions {
  city: string;
  country?: string;
  adminLevel: number;
}

export interface AreaSearchResult {
  selected: AreaCandidate | null;
  candidates: AreaCandidate[];
  ambiguous: boolean;
  areaId?: number;
}

function parsePopulation(value?: string): number | undefined {
  if (!value) {
    return undefined;
  }
  const numeric = Number(value.replace(/[^0-9]/g, ''));
  return Number.isFinite(numeric) ? numeric : undefined;
}

function buildCountryAreaClause(country: string | undefined): string {
  if (!country) {
    return '';
  }
  return `area["ISO3166-1"="${country}"]["boundary"="administrative"]["admin_level"="2"]->.country;`;
}

export function buildAreaQuery({ city, country, adminLevel }: AreaSearchOptions): string {
  const countryClause = buildCountryAreaClause(country);
  const areaFilter = country ? '(area.country)' : '';
  return `
[out:json][timeout:25];
${countryClause}
relation["boundary"="administrative"]["admin_level"="${adminLevel}"]["name"="${city}"]${areaFilter};
out tags;
`;
}

export async function fetchOverpass(query: string, options: OverpassQueryOptions = {}): Promise<OverpassResponse> {
  const endpoint = options.endpoint ?? DEFAULT_ENDPOINT;
  const timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;

  let attempt = 0;
  let lastError: Error | null = null;

  while (attempt < maxRetries) {
    attempt += 1;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
      logger.info({ endpoint, attempt }, 'Overpass request');
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'content-type': 'text/plain; charset=utf-8' },
        body: query,
        signal: controller.signal,
      });
      clearTimeout(timeout);

      if (!response.ok) {
        const body = await response.text();
        const error = new Error(`Overpass responded with ${response.status}: ${body}`);
        if (response.status === 429 || response.status >= 500) {
          lastError = error;
          const delay = 500 * 2 ** (attempt - 1);
          logger.warn({ status: response.status, delay, attempt }, 'Overpass retryable response');
          await new Promise((resolve) => setTimeout(resolve, delay));
          continue;
        }
        throw error;
      }

      const data = (await response.json()) as OverpassResponse;
      return data;
    } catch (error) {
      clearTimeout(timeout);
      lastError = error instanceof Error ? error : new Error('Overpass request failed');
      const delay = 500 * 2 ** (attempt - 1);
      logger.warn({ err: lastError, delay, attempt }, 'Overpass request failed');
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  throw lastError ?? new Error('Overpass request failed');
}

export async function findAdminArea(options: AreaSearchOptions): Promise<AreaSearchResult> {
  const query = buildAreaQuery(options);
  const response = await fetchOverpass(query);

  const candidates: AreaCandidate[] = response.elements
    .filter((element) => element.type === 'relation' && element.tags?.name)
    .map((element) => ({
      id: element.id,
      name: element.tags?.name ?? 'unknown',
      adminLevel: element.tags?.admin_level,
      population: parsePopulation(element.tags?.population),
      tags: element.tags ?? {},
    }));

  if (candidates.length === 0) {
    return { selected: null, candidates: [], ambiguous: false };
  }

  const normalizedTarget = options.city.toLowerCase();
  const exactMatches = candidates.filter((candidate) => candidate.name.toLowerCase() === normalizedTarget);

  const pool = exactMatches.length > 0 ? exactMatches : candidates;
  const sorted = [...pool].sort((a, b) => (b.population ?? 0) - (a.population ?? 0));
  const selected = sorted[0] ?? null;

  const ambiguous = pool.length > 1;
  const areaId = selected ? selected.id + 3_600_000_000 : undefined;

  return { selected, candidates: pool, ambiguous, areaId };
}

export function buildVenueQuery(areaId: number, amenity: string): string {
  return `
[out:json][timeout:25];
area(${areaId})->.searchArea;
(
  node["amenity"="${amenity}"](area.searchArea);
  way["amenity"="${amenity}"](area.searchArea);
  relation["amenity"="${amenity}"](area.searchArea);
);
out center tags;
`;
}

export function createOverpassLimiter(concurrency?: number): ReturnType<typeof pLimit> {
  return pLimit(Number.isFinite(concurrency) ? (concurrency as number) : DEFAULT_CONCURRENCY);
}
