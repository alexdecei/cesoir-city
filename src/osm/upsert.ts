import { writeCsv } from '../lib/csv.js';
import { logger } from '../lib/logger.js';
import { isSimilarAddress, roundCoord } from '../lib/normalize.js';
import {
  findVenuesByName,
  findVenueByOsm,
  insertVenue,
  updateVenue,
  VenuePayload,
  VenueRow,
} from '../lib/supabase.js';
import { NormalizedOsmVenue, OsmEntry } from './types.js';
import { toVenuePayload } from './fetch.js';

export type OsmUpsertAction = 'insert' | 'update' | 'conflict' | 'error';

export interface OsmUpsertRecord extends Record<string, unknown> {
  action: OsmUpsertAction;
  nom: string;
  adresse: string;
  city: string;
  latitude: number | null;
  longitude: number | null;
  reason?: string;
}

export interface OsmConflictRecord extends Record<string, unknown> {
  nom: string;
  existingAdresse: string;
  existingCity: string;
  newAdresse: string;
  newCity: string;
  reason: string;
}

export interface OsmUpsertOptions {
  dryRun: boolean;
  upsertCsvPath: string;
  conflictCsvPath: string;
  nowIso: string;
}

export interface OsmUpsertReport {
  processed: number;
  inserted: number;
  updated: number;
  conflicts: number;
  errors: number;
}

function buildOsmPayload(entry: NormalizedOsmVenue, nowIso: string): VenuePayload {
  return {
    ...toVenuePayload(entry, nowIso),
  };
}

function isCloseCoordinates(a: NormalizedOsmVenue, b: VenueRow): boolean {
  const radiusKm = 0.2;
  const toRad = (value: number): number => (value * Math.PI) / 180;
  const lat1 = toRad(a.latitude);
  const lat2 = toRad(b.latitude);
  const dLat = lat2 - lat1;
  const dLon = toRad(b.longitude) - toRad(a.longitude);
  const haversine =
    Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  const distance = 2 * 6371 * Math.asin(Math.sqrt(haversine));
  return distance <= radiusKm;
}


export async function processOsmUpserts(entries: OsmEntry[], options: OsmUpsertOptions): Promise<OsmUpsertReport> {
  const upsertRecords: OsmUpsertRecord[] = [];
  const conflictRecords: OsmConflictRecord[] = [];
  let inserted = 0;
  let updated = 0;
  let conflicts = 0;
  let errors = 0;

  for (const entry of entries) {
    if (entry.status !== 'ok' || !entry.venue) {
      continue;
    }

    try {
      const payload = buildOsmPayload(entry.venue, options.nowIso);
      const existingByOsm = await findVenueByOsm(entry.venue.osm_type, entry.venue.osm_id);

      if (existingByOsm) {
        const updatePayload: Omit<VenuePayload, 'nom'> = {
          adresse: payload.adresse,
          city: payload.city,
          latitude: payload.latitude,
          longitude: payload.longitude,
          osm_type: payload.osm_type,
          osm_id: payload.osm_id,
          osm_url: payload.osm_url,
          osm_tags_raw: payload.osm_tags_raw,
          address: payload.address,
          contact: payload.contact,
          osm_venue_type: payload.osm_venue_type,
          opening_hours: payload.opening_hours,
          capacity: payload.capacity,
          live_music: payload.live_music,
          website: payload.website,
          phone: payload.phone,
          instagram: payload.instagram,
          facebook: payload.facebook,
          source: payload.source,
          osm_last_sync_at: payload.osm_last_sync_at,
          tags: existingByOsm.tags && existingByOsm.tags.length > 0 ? undefined : payload.tags,
        };

        if (!options.dryRun) {
          await updateVenue(existingByOsm.id, updatePayload);
        }
        updated += 1;
        upsertRecords.push({
          action: 'update',
          nom: payload.nom,
          adresse: payload.adresse,
          city: payload.city,
          latitude: payload.latitude,
          longitude: payload.longitude,
        });
        continue;
      }

      const existingByName = await findVenuesByName(payload.nom);
      if (existingByName.length > 0) {
        const matching = existingByName.filter((candidate) => isSimilarAddress(candidate.adresse, payload.adresse, {
          existingCity: candidate.city,
          candidateCity: payload.city,
          candidatePostcode: entry.venue.postcode ?? undefined,
        }) && isCloseCoordinates(entry.venue, candidate));

        if (matching.length !== 1) {
          conflicts += 1;
          const conflictTarget = matching[0] ?? existingByName[0];
          conflictRecords.push({
            nom: payload.nom,
            existingAdresse: conflictTarget.adresse,
            existingCity: conflictTarget.city,
            newAdresse: payload.adresse,
            newCity: payload.city,
            reason: matching.length === 0 ? 'name_conflict_address_mismatch' : 'multiple_matches',
          });
          upsertRecords.push({
            action: 'conflict',
            nom: payload.nom,
            adresse: payload.adresse,
            city: payload.city,
            latitude: payload.latitude,
            longitude: payload.longitude,
            reason: matching.length === 0 ? 'name_conflict_address_mismatch' : 'multiple_matches',
          });
          continue;
        }

        const existing = matching[0];
        if (!options.dryRun) {
          await updateVenue(existing.id, {
            adresse: payload.adresse,
            city: payload.city,
            latitude: payload.latitude,
            longitude: payload.longitude,
            osm_type: payload.osm_type,
            osm_id: payload.osm_id,
            osm_url: payload.osm_url,
            osm_tags_raw: payload.osm_tags_raw,
            address: payload.address,
            contact: payload.contact,
            osm_venue_type: payload.osm_venue_type,
            opening_hours: payload.opening_hours,
            capacity: payload.capacity,
            live_music: payload.live_music,
            website: payload.website,
            phone: payload.phone,
            instagram: payload.instagram,
            facebook: payload.facebook,
            source: payload.source,
            osm_last_sync_at: payload.osm_last_sync_at,
            tags: existing.tags && existing.tags.length > 0 ? undefined : payload.tags,
          });
        }
        updated += 1;
        upsertRecords.push({
          action: 'update',
          nom: payload.nom,
          adresse: payload.adresse,
          city: payload.city,
          latitude: payload.latitude,
          longitude: payload.longitude,
          reason: 'name_match_address_match',
        });
        continue;
      }

      if (!options.dryRun) {
        await insertVenue(payload);
      }
      inserted += 1;
      upsertRecords.push({
        action: 'insert',
        nom: payload.nom,
        adresse: payload.adresse,
        city: payload.city,
        latitude: payload.latitude,
        longitude: payload.longitude,
      });
    } catch (error) {
      errors += 1;
      logger.error({ err: error, venue: entry.venue }, 'Failed to upsert OSM venue');
      upsertRecords.push({
        action: 'error',
        nom: entry.venue?.name ?? 'unknown',
        adresse: entry.venue?.adresse ?? 'unknown',
        city: entry.venue?.city ?? 'unknown',
        latitude: entry.venue ? roundCoord(entry.venue.latitude) : null,
        longitude: entry.venue ? roundCoord(entry.venue.longitude) : null,
        reason: error instanceof Error ? error.message : 'unknown_error',
      });
    }
  }

  await writeCsv(options.upsertCsvPath, upsertRecords, ['action', 'nom', 'adresse', 'city', 'latitude', 'longitude', 'reason']);
  await writeCsv(options.conflictCsvPath, conflictRecords, ['nom', 'existingAdresse', 'existingCity', 'newAdresse', 'newCity', 'reason']);

  return {
    processed: entries.filter((entry) => entry.status === 'ok').length,
    inserted,
    updated,
    conflicts,
    errors,
  };
}
