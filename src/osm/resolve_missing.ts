import { promises as fs } from 'fs';
import path from 'path';
import { normalizeName, normalizeCity, normalizeAddress } from '../utils/normalize.js';
import { stripEmpty } from '../export/dbShape.js';
import { buildUpsertSql } from '../utils/sql.js';
import { createNominatimClient, NominatimResult } from './nominatim.js';

export interface ResolveMissingOptions {
  inputPath: string;
  outDir: string;
  country: string;
  concurrency: number;
  delayMs: number;
  nameThreshold: number;
  addressThreshold: number;
}

interface InputVenue {
  nom: string;
  adresse?: string;
  city: string;
  postcode?: string;
}

interface ResolveRecord {
  input: InputVenue;
  osm: NominatimResult;
  dbVenue: Record<string, unknown>;
}

interface ResolveRecordAddress extends ResolveRecord {
  expectedName: string;
  foundName: string;
}

interface NotFoundRecord {
  input: InputVenue;
  reason: string;
}

interface ResolveReport {
  total: number;
  found_by_name: number;
  found_by_address: number;
  not_found: number;
  duration_ms: number;
}

function parseInput(content: string): InputVenue[] {
  const parsed = JSON.parse(content) as unknown;
  if (Array.isArray(parsed)) {
    return parsed as InputVenue[];
  }
  if (parsed && typeof parsed === 'object' && Array.isArray((parsed as { venues?: InputVenue[] }).venues)) {
    return (parsed as { venues: InputVenue[] }).venues;
  }
  throw new Error('Input must be a JSON array or object with key "venues"');
}

function parseAddress(adresse?: string): { housenumber?: string; street?: string } {
  if (!adresse) {
    return {};
  }
  const match = adresse.trim().match(/^\s*(\d+[A-Za-z]?)\s+(.+)$/);
  if (!match) {
    return {};
  }
  return { housenumber: match[1], street: match[2] };
}

function nameFromResult(result: NominatimResult): string {
  const named = result.namedetails?.name;
  if (named) {
    return named;
  }
  return result.display_name?.split(',')[0]?.trim() ?? '';
}

function isCityMatch(result: NominatimResult, city: string): boolean {
  const address = result.address ?? {};
  const candidateCity = address.city || address.town || address.village || address.municipality;
  if (!candidateCity) {
    return false;
  }
  return normalizeCity(candidateCity) === normalizeCity(city);
}

function isRelevantCategory(result: NominatimResult): boolean {
  const amenity = result.extratags?.amenity ?? result.type ?? '';
  const normalized = amenity.toLowerCase();
  const allowed = new Set([
    'bar',
    'pub',
    'nightclub',
    'theatre',
    'arts_centre',
    'casino',
    'community_centre',
    'concert_hall',
    'events_venue',
  ]);
  if (allowed.has(normalized)) {
    return true;
  }
  if (!amenity) {
    return true;
  }
  return ['bar', 'pub', 'nightclub', 'theatre', 'casino'].some((token) => normalized.includes(token));
}

function tokenOverlapScore(a: string[], b: string[]): number {
  if (!a.length || !b.length) {
    return 0;
  }
  const setA = new Set(a);
  const setB = new Set(b);
  const intersection = [...setA].filter((token) => setB.has(token));
  return Math.min(100, Math.round((intersection.length / Math.max(setA.size, setB.size)) * 100));
}

function scoreName(expected: string, result: NominatimResult): { score: number; reason: string } {
  const expectedNorm = normalizeName(expected);
  const found = nameFromResult(result);
  const foundNorm = normalizeName(found);
  if (!expectedNorm.normalized || !foundNorm.normalized) {
    return { score: 0, reason: 'empty' };
  }
  if (expectedNorm.normalized === foundNorm.normalized) {
    return { score: 100, reason: 'exact' };
  }
  if (expectedNorm.normalized.includes(foundNorm.normalized) || foundNorm.normalized.includes(expectedNorm.normalized)) {
    return { score: 70, reason: 'contains' };
  }
  const overlap = tokenOverlapScore(expectedNorm.tokens, foundNorm.tokens);
  return { score: overlap, reason: 'token_overlap' };
}

function scoreAddress(expected: InputVenue, result: NominatimResult): number {
  const address = result.address ?? {};
  const postcode = address.postcode;
  const road = address.road || address.pedestrian || address.path;
  const house = address.house_number;
  let score = 0;

  if (expected.postcode && postcode && expected.postcode === postcode) {
    score += 20;
  }
  if (expected.adresse && road) {
    const expectedNorm = normalizeAddress(expected.adresse);
    const roadNorm = normalizeAddress(`${house ?? ''} ${road}`.trim());
    if (expectedNorm.includes(roadNorm) || roadNorm.includes(expectedNorm)) {
      score += 20;
    }
  }
  return score;
}

function buildDbVenue(input: InputVenue, result: NominatimResult, nowIso: string): Record<string, unknown> {
  const lat = result.lat ? Number(result.lat) : undefined;
  const lon = result.lon ? Number(result.lon) : undefined;
  const address = result.address ?? {};
  const named = result.namedetails ?? {};
  const extratags = result.extratags ?? {};
  const stripTrailingSlashes = (value?: string): string | undefined => (value ? value.replace(/[\\/]+$/g, '') : undefined);

  const contact = stripEmpty({
    website: stripTrailingSlashes(extratags.website ?? extratags['contact:website'] ?? undefined),
    phone: extratags.phone ?? extratags['contact:phone'] ?? undefined,
    facebook: stripTrailingSlashes(extratags.facebook ?? extratags['contact:facebook'] ?? undefined),
    instagram: stripTrailingSlashes(extratags.instagram ?? extratags['contact:instagram'] ?? undefined),
  });

  const addressJson = stripEmpty({
    housenumber: address.house_number ?? undefined,
    street: address.road ?? address.pedestrian ?? address.path ?? undefined,
    postcode: address.postcode ?? undefined,
    city: address.city ?? address.town ?? address.village ?? address.municipality ?? undefined,
    full: result.display_name ?? undefined,
  });

  const adresse = addressJson.full ?? input.adresse ?? input.city;

  return stripEmpty({
    nom: nameFromResult(result) || input.nom,
    latitude: lat,
    longitude: lon,
    adresse,
    city: address.city ?? address.town ?? address.village ?? input.city,
    osm_type: result.osm_type,
    osm_id: result.osm_id,
    osm_url: result.osm_type && result.osm_id ? `https://www.openstreetmap.org/${result.osm_type}/${result.osm_id}` : undefined,
    osm_tags_raw: stripEmpty({ extratags, address, namedetails: named }),
    address: Object.keys(addressJson).length > 0 ? addressJson : undefined,
    contact: Object.keys(contact).length > 0 ? contact : undefined,
    osm_venue_type: extratags.amenity ?? result.type ?? undefined,
    opening_hours: extratags.opening_hours ?? undefined,
    capacity: extratags.capacity ? Number(extratags.capacity) : undefined,
    live_music: extratags.live_music === 'yes' ? true : extratags.live_music === 'no' ? false : undefined,
    website: stripTrailingSlashes(extratags.website ?? undefined),
    phone: extratags.phone ?? undefined,
    facebook: stripTrailingSlashes(extratags.facebook ?? undefined),
    instagram: stripTrailingSlashes(extratags.instagram ?? undefined),
    source: 'osm_resolve',
    osm_last_sync_at: nowIso,
  });
}

function selectBest(results: NominatimResult[], expected: InputVenue, nameThreshold: number): NominatimResult | null {
  const scored = results
    .filter((result) => result.lat && result.lon)
    .filter((result) => isCityMatch(result, expected.city))
    .filter((result) => isRelevantCategory(result))
    .map((result) => {
      const nameScore = scoreName(expected.nom, result).score;
      const addressScore = scoreAddress(expected, result);
      const cityBonus = 30;
      const postcodeBonus = expected.postcode && result.address?.postcode === expected.postcode ? 20 : 0;
      return {
        result,
        score: nameScore + cityBonus + postcodeBonus + addressScore,
      };
    })
    .sort((a, b) => b.score - a.score);

  if (!scored.length) {
    return null;
  }
  return scored[0].score >= nameThreshold ? scored[0].result : null;
}

function selectByAddress(results: NominatimResult[], expected: InputVenue, addressThreshold: number): NominatimResult | null {
  const scored = results
    .filter((result) => result.lat && result.lon)
    .filter((result) => isCityMatch(result, expected.city))
    .map((result) => ({
      result,
      score: scoreAddress(expected, result) + scoreName(expected.nom, result).score,
    }))
    .sort((a, b) => b.score - a.score);

  if (!scored.length) {
    return null;
  }
  return scored[0].score >= addressThreshold ? scored[0].result : null;
}

export async function runResolveMissing(options: ResolveMissingOptions): Promise<void> {
  const start = Date.now();
  await fs.mkdir(path.resolve(options.outDir), { recursive: true });
  const content = await fs.readFile(options.inputPath, 'utf8');
  const input = parseInput(content);

  const client = createNominatimClient({
    endpoint: process.env.NOMINATIM_ENDPOINT ?? 'https://nominatim.openstreetmap.org/search',
    cachePath: path.join(options.outDir, 'nominatim_cache.jsonl'),
    concurrency: options.concurrency,
    delayMs: options.delayMs,
  });

  const foundByName: ResolveRecord[] = [];
  const foundByAddress: ResolveRecordAddress[] = [];
  const notFound: NotFoundRecord[] = [];
  const dbVenues: Record<string, unknown>[] = [];
  const sqlStatements: string[] = [];
  const nowIso = new Date().toISOString();

  for (const venue of input) {
    const countrycodes = options.country.toLowerCase();
    const nameResults = await client({
      q: `${venue.nom} ${venue.city} ${options.country}`,
      format: 'jsonv2',
      addressdetails: '1',
      extratags: '1',
      namedetails: '1',
      limit: 10,
      countrycodes,
    });

    const bestByName = selectBest(nameResults, venue, options.nameThreshold);
    if (bestByName) {
      const dbVenue = buildDbVenue(venue, bestByName, nowIso);
      foundByName.push({ input: venue, osm: bestByName, dbVenue });
      dbVenues.push(dbVenue);
      sqlStatements.push(buildUpsertSql('public.venues', dbVenue));
      continue;
    }

    if (venue.adresse) {
      const parsedAddress = parseAddress(venue.adresse);
      const addressResults = await client({
        q: parsedAddress.street ? undefined : `${venue.adresse} ${venue.city}`,
        street: parsedAddress.street ? `${parsedAddress.housenumber ?? ''} ${parsedAddress.street}`.trim() : undefined,
        city: venue.city,
        postalcode: venue.postcode,
        format: 'jsonv2',
        addressdetails: '1',
        extratags: '1',
        namedetails: '1',
        limit: 10,
        countrycodes,
      });

      const bestByAddress = selectByAddress(addressResults, venue, options.addressThreshold);
      if (bestByAddress) {
        const dbVenue = buildDbVenue(venue, bestByAddress, nowIso);
        foundByAddress.push({
          input: venue,
          osm: bestByAddress,
          dbVenue,
          expectedName: venue.nom,
          foundName: nameFromResult(bestByAddress),
        });
        dbVenues.push(dbVenue);
        sqlStatements.push(buildUpsertSql('public.venues', dbVenue));
        continue;
      }
    }

    notFound.push({ input: venue, reason: 'no_match' });
  }

  await fs.writeFile(path.join(options.outDir, 'resolved.found_by_name.json'), JSON.stringify(foundByName, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'resolved.found_by_address.json'), JSON.stringify(foundByAddress, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'resolved.not_found.json'), JSON.stringify(notFound, null, 2), 'utf8');

  const venuesDbJsonl = dbVenues.map((record) => JSON.stringify(record)).join('\n');
  await fs.writeFile(path.join(options.outDir, 'venues.db.jsonl'), `${venuesDbJsonl}\n`, 'utf8');
  await fs.writeFile(path.join(options.outDir, 'inject.sql'), `${sqlStatements.join('\n')}\n`, 'utf8');

  const report: ResolveReport = {
    total: input.length,
    found_by_name: foundByName.length,
    found_by_address: foundByAddress.length,
    not_found: notFound.length,
    duration_ms: Date.now() - start,
  };

  await fs.writeFile(path.join(options.outDir, 'report.json'), JSON.stringify(report, null, 2), 'utf8');
}
