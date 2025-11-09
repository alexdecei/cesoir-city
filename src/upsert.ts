import { writeCsv } from './lib/csv.js';
import { logger } from './lib/logger.js';
import { isSimilarAddress, isSimilarName, normalizeAddress, roundCoord } from './lib/normalize.js';
import {
  findVenueByCity,
  findVenueByName,
  insertVenue,
  updateVenue,
  VenuePayload,
  VenueRow,
} from './lib/supabase.js';
import { GeocodedEntry } from './geocode.js';

export type UpsertAction = 'insert' | 'update' | 'conflict' | 'duplicate' | 'error';

export interface UpsertRecord extends Record<string, unknown> {
  action: UpsertAction;
  nom: string;
  adresse: string;
  city: string;
  latitude: number | null;
  longitude: number | null;
  reason?: string;
}

export interface ConflictRecord extends Record<string, unknown> {
  nom: string;
  existingAdresse: string;
  existingCity: string;
  newAdresse: string;
  newCity: string;
  reason: string;
}

export interface DuplicateRecord extends Record<string, unknown> {
  nom: string;
  duplicateNom: string;
  adresse: string;
  city: string;
  duplicateId: string;
  reason: string;
}

export interface UpsertOptions {
  dryRun: boolean;
  upsertCsvPath: string;
  conflictCsvPath: string;
  duplicateCsvPath: string;
}

export interface UpsertReport {
  processed: number;
  inserted: number;
  updated: number;
  conflicts: number;
  duplicates: number;
  errors: number;
}

function buildVenuePayload(entry: GeocodedEntry): VenuePayload {
  if (!entry.feature) {
    throw new Error('Cannot build payload without feature');
  }
  const { geometry, properties } = entry.feature;
  const [longitude, latitude] = geometry.coordinates;
  const city = properties.city || entry.input.ville;
  const adresse = properties.label ?? `${entry.input.adresse}, ${entry.input.ville}`;

  return {
    nom: entry.input.nom,
    adresse,
    city,
    latitude: roundCoord(latitude),
    longitude: roundCoord(longitude),
    barbars: true,
  };
}

function compareWithExisting(existing: VenueRow, entry: GeocodedEntry): boolean {
  if (!entry.feature) {
    return false;
  }
  const { properties } = entry.feature;
  return isSimilarAddress(existing.adresse, properties.label ?? entry.input.adresse, {
    existingCity: existing.city,
    candidateCity: properties.city ?? entry.input.ville,
    candidatePostcode: properties.postcode,
    candidateLabel: properties.label,
  });
}

export async function processUpserts(entries: GeocodedEntry[], options: UpsertOptions): Promise<UpsertReport> {
  const upsertRecords: UpsertRecord[] = [];
  const conflictRecords: ConflictRecord[] = [];
  const duplicateRecords: DuplicateRecord[] = [];
  let inserted = 0;
  let updated = 0;
  let conflicts = 0;
  let duplicates = 0;
  let errors = 0;

  const cityVenueCache = new Map<string, VenueRow[]>();

  async function getCityVenues(city: string): Promise<VenueRow[]> {
    if (!cityVenueCache.has(city)) {
      const rows = await findVenueByCity(city);
      cityVenueCache.set(city, rows);
    }
    return cityVenueCache.get(city) ?? [];
  }

  function updateCityCache(city: string, venue: VenueRow): void {
    const cache = cityVenueCache.get(city);
    if (!cache) {
      return;
    }
    const index = cache.findIndex((row) => row.id === venue.id);
    if (index >= 0) {
      cache[index] = venue;
    } else {
      cache.push(venue);
    }
  }

  for (const entry of entries) {
    if (entry.status !== 'ok' || !entry.feature) {
      continue;
    }

    try {
      const payload = buildVenuePayload(entry);
      const existing = await findVenueByName(entry.input.nom);

      if (!existing) {
        const candidates = await getCityVenues(payload.city);
        const normalizedPayloadAddress = normalizeAddress(payload.adresse);
        const duplicate = candidates.find((candidate) => {
          const sameAddress = normalizeAddress(candidate.adresse) === normalizedPayloadAddress;
          if (!sameAddress) {
            return false;
          }
          return isSimilarName(candidate.nom, payload.nom);
        });

        if (duplicate) {
          duplicates += 1;
          duplicateRecords.push({
            nom: payload.nom,
            duplicateNom: duplicate.nom,
            adresse: payload.adresse,
            city: payload.city,
            duplicateId: duplicate.id,
            reason: 'address_name_similarity',
          });
          upsertRecords.push({
            action: 'duplicate',
            nom: payload.nom,
            adresse: payload.adresse,
            city: payload.city,
            latitude: payload.latitude,
            longitude: payload.longitude,
            reason: 'address_name_similarity',
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

        if (cityVenueCache.has(payload.city)) {
          cityVenueCache.get(payload.city)?.push({
            id: `temp-${Date.now()}-${Math.random()}`,
            nom: payload.nom,
            adresse: payload.adresse,
            city: payload.city,
            latitude: payload.latitude,
            longitude: payload.longitude,
            barbars: payload.barbars,
          });
        }
        continue;
      }

      const similar = compareWithExisting(existing, entry);

      if (!similar) {
        conflicts += 1;
        const newAdresse = payload.adresse;
        conflictRecords.push({
          nom: payload.nom,
          existingAdresse: existing.adresse,
          existingCity: existing.city,
          newAdresse,
          newCity: payload.city,
          reason: 'address_mismatch',
        });
        upsertRecords.push({
          action: 'conflict',
          nom: payload.nom,
          adresse: newAdresse,
          city: payload.city,
          latitude: payload.latitude,
          longitude: payload.longitude,
          reason: 'address_mismatch',
        });
        continue;
      }

      const updatePayload: Omit<VenuePayload, 'nom'> = {
        adresse: payload.adresse,
        city: payload.city,
        latitude: payload.latitude,
        longitude: payload.longitude,
        barbars: true,
      };

      if (!options.dryRun) {
        await updateVenue(existing.id, updatePayload);
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

      updateCityCache(payload.city, {
        ...existing,
        adresse: updatePayload.adresse,
        city: updatePayload.city,
        latitude: updatePayload.latitude,
        longitude: updatePayload.longitude,
        barbars: updatePayload.barbars,
      });
    } catch (error) {
      errors += 1;
      logger.error({ err: error, nom: entry.input.nom }, 'Failed to upsert venue');
      upsertRecords.push({
        action: 'error',
        nom: entry.input.nom,
        adresse: entry.input.adresse,
        city: entry.input.ville,
        latitude: entry.feature ? roundCoord(entry.feature.geometry.coordinates[1]) : null,
        longitude: entry.feature ? roundCoord(entry.feature.geometry.coordinates[0]) : null,
        reason: error instanceof Error ? error.message : 'unknown_error',
      });
    }
  }

  await writeCsv(options.upsertCsvPath, upsertRecords, ['action', 'nom', 'adresse', 'city', 'latitude', 'longitude', 'reason']);
  await writeCsv(options.conflictCsvPath, conflictRecords, ['nom', 'existingAdresse', 'existingCity', 'newAdresse', 'newCity', 'reason']);
  await writeCsv(options.duplicateCsvPath, duplicateRecords, ['nom', 'duplicateNom', 'adresse', 'city', 'duplicateId', 'reason']);

  return {
    processed: entries.filter((entry) => entry.status === 'ok').length,
    inserted,
    updated,
    conflicts,
    duplicates,
    errors,
  };
}
