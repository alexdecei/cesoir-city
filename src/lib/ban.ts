import { promises as fs } from 'fs';
import { createReadStream } from 'fs';
import { once } from 'events';
import readline from 'readline';
import pLimit from 'p-limit';
import path from 'path';
import { logger } from './logger.js';

const BAN_BASE_URL = 'https://api-adresse.data.gouv.fr/search/';

export interface BanFeatureProperties {
  label: string;
  score: number;
  housenumber?: string;
  id: string;
  name?: string;
  postcode?: string;
  citycode: string;
  city: string;
  context?: string;
  type: string;
  importance?: number;
  street?: string;
}

export interface BanGeometry {
  type: 'Point';
  coordinates: [number, number];
}

export interface BanFeature {
  type: 'Feature';
  geometry: BanGeometry;
  properties: BanFeatureProperties;
}

export interface BanResponse {
  type: 'FeatureCollection';
  features: BanFeature[];
}

export interface GeocodeResult {
  feature: BanFeature | null;
  fromCache: boolean;
}

interface CacheRecord {
  key: string;
  feature: BanFeature | null;
  timestamp: string;
}

export interface BanClientOptions {
  cachePath: string;
  concurrency: number;
  useCache: boolean;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function fetchWithRetry(url: string, attempt = 0, maxAttempts = 3, timeoutMs = 5000): Promise<BanResponse> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, { signal: controller.signal });

    if (!response.ok) {
      if ((response.status >= 500 || response.status === 429) && attempt < maxAttempts - 1) {
        const delay = 2 ** attempt * 200;
        await sleep(delay);
        return fetchWithRetry(url, attempt + 1, maxAttempts, timeoutMs);
      }
      throw new Error(`BAN request failed with status ${response.status}`);
    }

    return (await response.json()) as BanResponse;
  } catch (error) {
    if (attempt < maxAttempts - 1) {
      const delay = 2 ** attempt * 200;
      await sleep(delay);
      return fetchWithRetry(url, attempt + 1, maxAttempts, timeoutMs);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

export class BanClient {
  private cache = new Map<string, CacheRecord>();
  private ready: Promise<void> | null = null;
  private limit;
  private cachePath: string;
  private cacheDir: string;

  constructor(private readonly options: BanClientOptions) {
    this.limit = pLimit(options.concurrency);
    this.cachePath = path.resolve(options.cachePath);
    this.cacheDir = path.dirname(this.cachePath);
    if (options.useCache) {
      this.ready = this.loadCache();
    }
  }

  private async loadCache(): Promise<void> {
    try {
      const stream = createReadStream(this.cachePath, { encoding: 'utf8' });
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

      rl.on('line', (line) => {
        try {
          if (!line.trim()) {
            return;
          }
          const record = JSON.parse(line) as CacheRecord;
          this.cache.set(record.key, record);
        } catch (error) {
          logger.warn({ err: error }, 'Failed to parse cache line');
        }
      });

      await once(rl, 'close');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') {
        logger.warn({ err: error }, 'Unable to load BAN cache');
      }
    }
  }

  private async ensureReady(): Promise<void> {
    if (this.ready) {
      await this.ready;
    }
  }

  private buildKey(adresse: string, ville: string, postcode?: string): string {
    return `${adresse.trim()}|${ville.trim()}|${postcode?.trim() ?? ''}`;
  }

  async geocode(adresse: string, ville: string, postcode?: string): Promise<GeocodeResult> {
    await this.ensureReady();
    const key = this.buildKey(adresse, ville, postcode);

    if (this.cache.has(key)) {
      const cached = this.cache.get(key)!;
      return { feature: cached.feature, fromCache: true };
    }

    const queryParts = [`${adresse} ${ville}`.trim()];
    const params = new URLSearchParams({ q: queryParts.join(' '), limit: '1' });
    if (postcode) {
      params.set('postcode', postcode);
    }

    const url = `${BAN_BASE_URL}?${params.toString()}`;

    const response = await this.limit(() => fetchWithRetry(url));
    const feature = response.features[0] ?? null;

    const record: CacheRecord = {
      key,
      feature,
      timestamp: new Date().toISOString(),
    };

    this.cache.set(key, record);

    try {
      await fs.mkdir(this.cacheDir, { recursive: true });
      await fs.appendFile(this.cachePath, `${JSON.stringify(record)}\n`, 'utf8');
    } catch (error) {
      logger.warn({ err: error }, 'Unable to persist BAN cache');
    }

    return { feature, fromCache: false };
  }
}
