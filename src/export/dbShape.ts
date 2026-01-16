import { promises as fs } from 'fs';
import path from 'path';
import { NormalizedOsmVenue } from '../osm/types.js';

export interface DbVenueExport {
  nom: string;
  latitude: number;
  longitude: number;
  adresse: string;
  city: string;
  tags?: string[];
  description?: string;
  event_max?: number;
  programmation?: string;
  lastchecked?: string;
  plan?: string;
  instagram?: string;
  website?: string;
  phone?: string;
  facebook?: string;
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
  source: 'osm_seed';
  osm_last_sync_at?: string;
}

export function stripEmpty<T extends Record<string, unknown>>(value: T): T {
  const entries = Object.entries(value).flatMap(([key, val]) => {
    if (val === null || val === undefined) {
      return [];
    }
    if (Array.isArray(val)) {
      return val.length === 0 ? [] : [[key, val]];
    }
    if (typeof val === 'object') {
      const cleaned = stripEmpty(val as Record<string, unknown>);
      return Object.keys(cleaned).length === 0 ? [] : [[key, cleaned]];
    }
    return [[key, val]];
  });

  return Object.fromEntries(entries) as T;
}

function parseLiveMusic(raw?: string | null): boolean | undefined {
  if (!raw) {
    return undefined;
  }
  const normalized = raw.toLowerCase();
  if (normalized === 'yes') {
    return true;
  }
  if (normalized === 'no') {
    return false;
  }
  return undefined;
}

function parseCapacity(raw?: string | null): number | undefined {
  if (!raw) {
    return undefined;
  }
  const numeric = Number(raw.replace(/[^0-9]/g, ''));
  return Number.isFinite(numeric) && numeric > 0 ? numeric : undefined;
}

export function toDbVenue(venue: NormalizedOsmVenue, nowIso: string): DbVenueExport {
  const address = stripEmpty({
    housenumber: venue.address.housenumber ?? undefined,
    street: venue.address.street ?? undefined,
    postcode: venue.address.postcode ?? undefined,
    city: venue.address.city ?? undefined,
    full: venue.address.full ?? undefined,
  });

  const contact = stripEmpty({
    website: venue.contact.website ?? undefined,
    phone: venue.contact.phone ?? undefined,
    instagram: venue.contact.instagram ?? undefined,
    facebook: venue.contact.facebook ?? undefined,
  });

  const dbVenue: DbVenueExport = {
    nom: venue.name,
    latitude: venue.latitude,
    longitude: venue.longitude,
    adresse: venue.adresse,
    city: venue.city,
    tags: venue.type ? [venue.type] : undefined,
    osm_type: venue.osm_type,
    osm_id: venue.osm_id,
    osm_url: venue.osm_url,
    osm_tags_raw: venue.osm_tags_raw,
    address: Object.keys(address).length > 0 ? address : undefined,
    contact: Object.keys(contact).length > 0 ? contact : undefined,
    osm_venue_type: venue.type ?? undefined,
    opening_hours: venue.opening_hours ?? undefined,
    capacity: parseCapacity(venue.capacity ?? undefined),
    live_music: parseLiveMusic(venue.live_music ?? undefined),
    instagram: venue.contact.instagram ?? undefined,
    website: venue.contact.website ?? undefined,
    phone: venue.contact.phone ?? undefined,
    facebook: venue.contact.facebook ?? undefined,
    source: 'osm_seed',
    osm_last_sync_at: nowIso,
  };

  return stripEmpty(dbVenue);
}

export async function writeDbExport(
  venues: NormalizedOsmVenue[],
  outPath: string,
  nowIso: string,
): Promise<void> {
  await fs.mkdir(path.dirname(path.resolve(outPath)), { recursive: true });
  const lines = venues.map((venue) => JSON.stringify(toDbVenue(venue, nowIso)));
  await fs.writeFile(outPath, `${lines.join('\n')}\n`, 'utf8');
}
