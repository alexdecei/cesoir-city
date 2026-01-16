import pLimit from 'p-limit';
import { logger } from '../lib/logger.js';
import { OverpassResponse } from './types.js';

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
