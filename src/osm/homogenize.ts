import { promises as fs } from 'fs';
import path from 'path';
import { ensureClient } from '../lib/supabase.js';
import { logger } from '../lib/logger.js';
import { stripEmpty } from '../export/dbShape.js';

const STOPWORDS = new Set([
  'le',
  'la',
  'les',
  'l',
  "l'",
  'au',
  'aux',
  'du',
  'de',
  'des',
  'd',
  "d'",
]);

export interface HomogenizeOptions {
  city: string;
  inputPath: string;
  outDir: string;
  distanceThresholdM: number;
}

export interface BddVenueRow {
  id: string;
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  osm_type: string | null;
  osm_id: number | null;
  osm_url: string | null;
  osm_tags_raw: Record<string, unknown> | null;
  address: Record<string, unknown> | null;
  contact: Record<string, unknown> | null;
  osm_venue_type: string | null;
  opening_hours: string | null;
  capacity: number | null;
  live_music: boolean | null;
  website: string | null;
  phone: string | null;
  facebook: string | null;
  instagram: string | null;
  source: string | null;
}

export interface OsmCandidate {
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  osm_type?: string;
  osm_id?: number;
  osm_url?: string;
  osm_tags_raw?: Record<string, unknown>;
  address?: Record<string, unknown>;
  contact?: Record<string, unknown>;
  osm_venue_type?: string;
  opening_hours?: string;
  capacity?: number;
  live_music?: boolean;
  website?: string;
  phone?: string;
  facebook?: string;
  instagram?: string;
}

export interface MatchRecord {
  bdd: BddVenueRow;
  osm: OsmCandidate;
  patch: Record<string, unknown>;
  match: {
    normalized_name: string;
    distance_m?: number;
  };
}

export interface AmbiguousRecord {
  reason: string;
  normalized_name: string;
  bdd_candidates: BddVenueRow[];
  osm_candidates: OsmCandidate[];
}

export interface HomogenizeReport {
  bdd_total: number;
  osm_total: number;
  matched: number;
  ambiguous: number;
  solo_osm: number;
  solo_bdd: number;
  duration_ms: number;
}

function normalizeName(input: string): string {
  const normalized = input
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}]+/gu, ' ')
    .replace(/\b([ld])\b/g, ' ')
    .trim();

  if (!normalized) {
    return '';
  }

  const tokens = normalized
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token && !STOPWORDS.has(token));

  return tokens.join(' ').trim();
}

function haversineDistanceMeters(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRad = (value: number): number => (value * Math.PI) / 180;
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function isEmptyValue(value: unknown): boolean {
  if (value === null || value === undefined) {
    return true;
  }
  if (typeof value === 'string') {
    return value.trim().length === 0;
  }
  if (Array.isArray(value)) {
    return value.length === 0;
  }
  if (typeof value === 'object') {
    return Object.keys(value as Record<string, unknown>).length === 0;
  }
  return false;
}

function maybeAddressReplacement(bdd: BddVenueRow, osm: OsmCandidate): string | undefined {
  const current = bdd.adresse?.trim();
  if (!current) {
    return osm.adresse;
  }
  if (current === bdd.city && osm.adresse && osm.adresse !== bdd.city) {
    return osm.adresse;
  }
  return undefined;
}

function buildPatch(bdd: BddVenueRow, osm: OsmCandidate, nowIso: string): Record<string, unknown> {
  const patch: Record<string, unknown> = {};

  if (isEmptyValue(bdd.osm_type) && osm.osm_type) {
    patch.osm_type = osm.osm_type;
  }
  if (isEmptyValue(bdd.osm_id) && osm.osm_id) {
    patch.osm_id = osm.osm_id;
  }
  if (isEmptyValue(bdd.osm_url) && osm.osm_url) {
    patch.osm_url = osm.osm_url;
  }
  if (isEmptyValue(bdd.osm_tags_raw) && osm.osm_tags_raw) {
    patch.osm_tags_raw = osm.osm_tags_raw;
  }
  if (isEmptyValue(bdd.address) && osm.address && Object.keys(osm.address).length > 0) {
    patch.address = osm.address;
  }
  if (isEmptyValue(bdd.contact) && osm.contact && Object.keys(osm.contact).length > 0) {
    patch.contact = osm.contact;
  }
  if (isEmptyValue(bdd.osm_venue_type) && osm.osm_venue_type) {
    patch.osm_venue_type = osm.osm_venue_type;
  }
  if (isEmptyValue(bdd.opening_hours) && osm.opening_hours) {
    patch.opening_hours = osm.opening_hours;
  }
  if (isEmptyValue(bdd.capacity) && osm.capacity !== undefined) {
    patch.capacity = osm.capacity;
  }
  if (isEmptyValue(bdd.live_music) && typeof osm.live_music === 'boolean') {
    patch.live_music = osm.live_music;
  }
  if (isEmptyValue(bdd.website) && osm.website) {
    patch.website = osm.website;
  }
  if (isEmptyValue(bdd.phone) && osm.phone) {
    patch.phone = osm.phone;
  }
  if (isEmptyValue(bdd.facebook) && osm.facebook) {
    patch.facebook = osm.facebook;
  }
  if (isEmptyValue(bdd.instagram) && osm.instagram) {
    patch.instagram = osm.instagram;
  }

  const replacementAdresse = maybeAddressReplacement(bdd, osm);
  if (replacementAdresse) {
    patch.adresse = replacementAdresse;
  }

  const latEmpty = bdd.latitude === 0 || Number.isNaN(bdd.latitude);
  const lonEmpty = bdd.longitude === 0 || Number.isNaN(bdd.longitude);
  if ((latEmpty || lonEmpty) && osm.latitude !== undefined && osm.longitude !== undefined) {
    patch.latitude = osm.latitude;
    patch.longitude = osm.longitude;
  }

  if (isEmptyValue(bdd.source)) {
    patch.source = 'osm_seed';
  }

  if (Object.keys(patch).length > 0) {
    patch.osm_last_sync_at = nowIso;
  }

  return stripEmpty(patch);
}

function parseJsonLines(content: string): Record<string, unknown>[] {
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function toOsmCandidate(record: Record<string, unknown>, fallbackCity: string): OsmCandidate | null {
  const nom = typeof record.nom === 'string' ? record.nom : typeof record.name === 'string' ? record.name : undefined;
  if (!nom) {
    return null;
  }

  const latitude = typeof record.latitude === 'number' ? record.latitude : typeof record.lat === 'number' ? record.lat : undefined;
  const longitude = typeof record.longitude === 'number' ? record.longitude : typeof record.lon === 'number' ? record.lon : undefined;
  if (latitude === undefined || longitude === undefined) {
    return null;
  }

  const adresse = typeof record.adresse === 'string'
    ? record.adresse
    : typeof record.address === 'string'
      ? record.address
      : fallbackCity;

  const city = typeof record.city === 'string' ? record.city : fallbackCity;

  const contact = (record.contact ?? {}) as Record<string, unknown>;
  const address = (record.address ?? {}) as Record<string, unknown>;

  return stripEmpty({
    nom,
    adresse,
    city,
    latitude,
    longitude,
    osm_type: typeof record.osm_type === 'string' ? record.osm_type : undefined,
    osm_id: typeof record.osm_id === 'number' ? record.osm_id : undefined,
    osm_url: typeof record.osm_url === 'string' ? record.osm_url : undefined,
    osm_tags_raw: typeof record.osm_tags_raw === 'object' && record.osm_tags_raw !== null ? record.osm_tags_raw as Record<string, unknown> : undefined,
    address: Object.keys(address).length > 0 ? address : undefined,
    contact: Object.keys(contact).length > 0 ? contact : undefined,
    osm_venue_type: typeof record.osm_venue_type === 'string' ? record.osm_venue_type : undefined,
    opening_hours: typeof record.opening_hours === 'string' ? record.opening_hours : undefined,
    capacity: typeof record.capacity === 'number' ? record.capacity : undefined,
    live_music: typeof record.live_music === 'boolean' ? record.live_music : undefined,
    website: typeof record.website === 'string' ? record.website : undefined,
    phone: typeof record.phone === 'string' ? record.phone : undefined,
    facebook: typeof record.facebook === 'string' ? record.facebook : undefined,
    instagram: typeof record.instagram === 'string' ? record.instagram : undefined,
  });
}

function sortByName<T extends { nom: string }>(items: T[]): T[] {
  return [...items].sort((a, b) => a.nom.localeCompare(b.nom, 'fr', { sensitivity: 'base' }));
}

function escapeSqlValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : 'NULL';
  }
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }
  if (Array.isArray(value)) {
    const escaped = value
      .map((item) => `'${String(item).replace(/'/g, "''")}'`)
      .join(',');
    return `ARRAY[${escaped}]`;
  }
  if (typeof value === 'object') {
    const json = JSON.stringify(value).replace(/'/g, "''");
    return `'${json}'::jsonb`;
  }
  const escaped = String(value).replace(/'/g, "''");
  return `'${escaped}'`;
}

function buildSqlUpdate(id: string, patch: Record<string, unknown>): string {
  const entries = Object.entries(patch);
  const assignments = entries.map(([key, value]) => `${key} = ${escapeSqlValue(value)}`);
  assignments.push('updated_at = now()');
  return `UPDATE public.venues SET ${assignments.join(', ')} WHERE id = '${id}';`;
}

export async function runHomogenize(options: HomogenizeOptions): Promise<void> {
  const start = Date.now();
  await fs.mkdir(path.resolve(options.outDir), { recursive: true });

  const inputContent = await fs.readFile(options.inputPath, 'utf8');
  const trimmed = inputContent.trim();
  const parsedInput = trimmed.startsWith('[')
    ? (JSON.parse(trimmed) as Record<string, unknown>[])
    : parseJsonLines(inputContent);

  const osmCandidates = parsedInput
    .map((record) => toOsmCandidate(record, options.city))
    .filter((candidate): candidate is OsmCandidate => candidate !== null);

  const supabase = ensureClient();
  const { data, error } = await supabase
    .from('venues')
    .select('id, nom, adresse, city, latitude, longitude, osm_type, osm_id, osm_url, osm_tags_raw, address, contact, osm_venue_type, opening_hours, capacity, live_music, website, phone, facebook, instagram, source')
    .or('osm_id.is.null,osm_type.is.null')
    .ilike('city', options.city);

  if (error) {
    logger.error({ err: error }, 'Failed to fetch BDD venues');
    throw error;
  }

  const bddVenues = (data ?? []) as BddVenueRow[];

  const osmMap = new Map<string, OsmCandidate[]>();
  for (const osm of osmCandidates) {
    const normalized = normalizeName(osm.nom);
    if (!normalized) {
      continue;
    }
    const list = osmMap.get(normalized) ?? [];
    list.push(osm);
    osmMap.set(normalized, list);
  }

  const bddMap = new Map<string, BddVenueRow[]>();
  for (const bdd of bddVenues) {
    const normalized = normalizeName(bdd.nom);
    if (!normalized) {
      continue;
    }
    const list = bddMap.get(normalized) ?? [];
    list.push(bdd);
    bddMap.set(normalized, list);
  }

  const matched: MatchRecord[] = [];
  const ambiguous: AmbiguousRecord[] = [];
  const matchedBddIds = new Set<string>();
  const matchedOsmKeys = new Set<string>();
  const nowIso = new Date().toISOString();

  for (const [normalizedName, bddCandidates] of bddMap.entries()) {
    const osmMatches = osmMap.get(normalizedName) ?? [];

    if (bddCandidates.length !== 1 || osmMatches.length !== 1) {
      if (bddCandidates.length > 0 || osmMatches.length > 0) {
        ambiguous.push({
          reason: bddCandidates.length !== 1 && osmMatches.length !== 1
            ? 'multiple_bdd_multiple_osm'
            : bddCandidates.length !== 1
              ? 'multiple_bdd'
              : 'multiple_osm',
          normalized_name: normalizedName,
          bdd_candidates: bddCandidates,
          osm_candidates: osmMatches,
        });
      }
      continue;
    }

    const bdd = bddCandidates[0];
    const osm = osmMatches[0];

    let distance: number | undefined;
    if (bdd.latitude && bdd.longitude && osm.latitude && osm.longitude) {
      distance = haversineDistanceMeters(bdd.latitude, bdd.longitude, osm.latitude, osm.longitude);
      if (distance > options.distanceThresholdM) {
        ambiguous.push({
          reason: 'distance_threshold',
          normalized_name: normalizedName,
          bdd_candidates: [bdd],
          osm_candidates: [osm],
        });
        continue;
      }
    }

    const patch = buildPatch(bdd, osm, nowIso);
    matched.push({
      bdd,
      osm,
      patch,
      match: { normalized_name: normalizedName, distance_m: distance },
    });
    matchedBddIds.add(bdd.id);
    matchedOsmKeys.add(`${osm.osm_type ?? 'unknown'}:${osm.osm_id ?? osm.nom}`);
  }

  const soloBdd = bddVenues.filter((bdd) => !matchedBddIds.has(bdd.id));
  const soloOsm = osmCandidates.filter((osm) => !matchedOsmKeys.has(`${osm.osm_type ?? 'unknown'}:${osm.osm_id ?? osm.nom}`));

  const matchedSorted = matched.sort((a, b) => a.bdd.nom.localeCompare(b.bdd.nom, 'fr', { sensitivity: 'base' }));
  const ambiguousSorted = ambiguous.sort((a, b) => a.normalized_name.localeCompare(b.normalized_name, 'fr', { sensitivity: 'base' }));

  await fs.writeFile(path.join(options.outDir, 'matched.json'), JSON.stringify(matchedSorted, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'ambiguous.json'), JSON.stringify(ambiguousSorted, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'solo_osm.json'), JSON.stringify(sortByName(soloOsm), null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'solo_bdd.json'), JSON.stringify(sortByName(soloBdd), null, 2), 'utf8');

  const sqlStatements = matchedSorted
    .filter((record) => Object.keys(record.patch).length > 0)
    .map((record) => buildSqlUpdate(record.bdd.id, record.patch))
    .join('\n');

  await fs.writeFile(path.join(options.outDir, 'homogenize_nantes.sql'), `${sqlStatements}\n`, 'utf8');

  const report: HomogenizeReport = {
    bdd_total: bddVenues.length,
    osm_total: osmCandidates.length,
    matched: matchedSorted.length,
    ambiguous: ambiguousSorted.length,
    solo_osm: soloOsm.length,
    solo_bdd: soloBdd.length,
    duration_ms: Date.now() - start,
  };

  await fs.writeFile(path.join(options.outDir, 'report.json'), JSON.stringify(report, null, 2), 'utf8');
}
