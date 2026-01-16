import { promises as fs } from 'fs';
import path from 'path';
import pLimit from 'p-limit';
import { logger } from '../lib/logger.js';

export interface NominatimResult {
  place_id?: number;
  osm_type?: string;
  osm_id?: number;
  lat?: string;
  lon?: string;
  display_name?: string;
  class?: string;
  type?: string;
  address?: Record<string, string>;
  extratags?: Record<string, string>;
  namedetails?: Record<string, string>;
}

export interface NominatimSearchParams {
  q?: string;
  street?: string;
  city?: string;
  postalcode?: string;
  countrycodes?: string;
  format: 'jsonv2';
  addressdetails: '1';
  extratags: '1';
  namedetails: '1';
  limit: number;
}

export interface NominatimClientOptions {
  endpoint: string;
  cachePath: string;
  concurrency: number;
  delayMs: number;
}

export function buildQuery(params: NominatimSearchParams): string {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === '') {
      return;
    }
    query.set(key, String(value));
  });
  return query.toString();
}

function buildCacheKey(params: NominatimSearchParams): string {
  return JSON.stringify(params);
}

async function readCache(cachePath: string): Promise<Map<string, NominatimResult[]>> {
  const cache = new Map<string, NominatimResult[]>();
  try {
    const content = await fs.readFile(cachePath, 'utf8');
    content
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .forEach((line) => {
        const parsed = JSON.parse(line) as { key: string; response: NominatimResult[] };
        if (parsed.key) {
          cache.set(parsed.key, parsed.response ?? []);
        }
      });
  } catch (error) {
    return cache;
  }
  return cache;
}

async function appendCache(cachePath: string, key: string, response: NominatimResult[]): Promise<void> {
  await fs.mkdir(path.dirname(path.resolve(cachePath)), { recursive: true });
  await fs.appendFile(cachePath, `${JSON.stringify({ key, response })}\n`, 'utf8');
}

async function delay(ms: number): Promise<void> {
  if (ms <= 0) {
    return;
  }
  await new Promise((resolve) => setTimeout(resolve, ms));
}

export function createNominatimClient(options: NominatimClientOptions) {
  const limiter = pLimit(options.concurrency);
  const cachePromise = readCache(options.cachePath);

  return async (params: NominatimSearchParams): Promise<NominatimResult[]> => {
    const cache = await cachePromise;
    const key = buildCacheKey(params);
    if (cache.has(key)) {
      return cache.get(key) ?? [];
    }

    return limiter(async () => {
      await delay(options.delayMs);
      const query = buildQuery(params);
      const url = `${options.endpoint}?${query}`;

      let attempt = 0;
      let lastError: Error | null = null;

      while (attempt < 3) {
        attempt += 1;
        try {
          const response = await fetch(url, {
            headers: {
              'user-agent': 'cesoir-city/1.0 (contact: ops@cesoir.city)',
            },
          });
          if (!response.ok) {
            const body = await response.text();
            const error = new Error(`Nominatim responded with ${response.status}: ${body}`);
            if (response.status === 429 || response.status >= 500) {
              lastError = error;
              await delay(500 * 2 ** (attempt - 1));
              continue;
            }
            throw error;
          }
          const data = (await response.json()) as NominatimResult[];
          cache.set(key, data);
          await appendCache(options.cachePath, key, data);
          return data;
        } catch (error) {
          lastError = error instanceof Error ? error : new Error('Nominatim request failed');
          logger.warn({ err: lastError, attempt }, 'Nominatim request failed');
          await delay(500 * 2 ** (attempt - 1));
        }
      }

      throw lastError ?? new Error('Nominatim request failed');
    });
  };
}
