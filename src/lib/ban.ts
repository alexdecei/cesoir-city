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
    logger.info({ url, attempt }, 'Sending BAN request');
    console.log(`[BAN] Request attempt ${attempt + 1}/${maxAttempts}: ${url}`);
    const response = await fetch(url, { signal: controller.signal });

    if (!response.ok) {
      let bodyText: string | undefined;
      try {
        bodyText = await response.text();
      } catch (bodyError) {
        logger.debug({ err: bodyError }, 'Unable to read BAN error body');
      }

      logger.warn({ url, status: response.status, body: bodyText }, 'BAN request returned non-OK status');
      console.log(`[BAN] Non-OK response (${response.status}) for ${url}`);
      if (bodyText) {
        console.log(`[BAN] Response body: ${bodyText}`);
      }

      if ((response.status >= 500 || response.status === 429) && attempt < maxAttempts - 1) {
        const delay = 2 ** attempt * 200;
        await sleep(delay);
        console.log(`[BAN] Retrying after ${delay}ms for ${url}`);
        return fetchWithRetry(url, attempt + 1, maxAttempts, timeoutMs);
      }
      const error = new Error(`BAN request failed with status ${response.status}`);
      (error as Error & { responseBody?: string }).responseBody = bodyText;
      throw error;
    }

    const json = (await response.json()) as Partial<BanResponse> & { error?: unknown };
    console.log(`[BAN] Successful response for ${url}`);

    if (!json || !Array.isArray(json.features)) {
      logger.error({ url, response: json }, 'Unexpected BAN response shape');
      console.log(`[BAN] Invalid response shape for ${url}`);
      throw new Error('Invalid BAN response: missing features array');
    }

    return json as BanResponse;
  } catch (error) {
    if (attempt < maxAttempts - 1) {
      const delay = 2 ** attempt * 200;
      await sleep(delay);
      logger.debug({ url, attempt, delay }, 'Retrying BAN request');
      console.log(`[BAN] Error on attempt ${attempt + 1} for ${url}, retrying in ${delay}ms`);
      return fetchWithRetry(url, attempt + 1, maxAttempts, timeoutMs);
    }
    logger.error({ url, err: error }, 'BAN request failed after retries');
    console.log(`[BAN] Request failed after ${maxAttempts} attempts for ${url}`);
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
      logger.debug({ key }, 'BAN cache hit');
      return { feature: cached.feature, fromCache: true };
    }

    const query = [adresse, ville].filter(Boolean).join(' ').trim();
    if (!query) {
      logger.warn({ adresse, ville }, 'Cannot geocode entry with empty query');
      return { feature: null, fromCache: false };
    }

    const params = new URLSearchParams({ q: query, limit: '1' });
    if (postcode) {
      params.set('postcode', postcode);
    }

    const url = `${BAN_BASE_URL}?${params.toString()}`;

    console.log(`[BAN] Final request URL: ${url}`);
    const response = await this.limit(() => fetchWithRetry(url));
    const feature = response.features[0] ?? null;

    if (!feature) {
      logger.info({ key, url }, 'BAN returned no feature');
      console.log(`[BAN] No feature returned for ${url}`);
    } else {
      logger.debug({ key, url, score: feature.properties.score }, 'BAN returned feature');
      console.log(
        `[BAN] Feature returned for ${url} with score ${feature.properties.score} and label ${feature.properties.label}`,
      );
    }

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
