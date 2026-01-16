import { normalizeString, roundCoord } from '../lib/normalize.js';
import { NormalizedOsmVenue, OverpassElement, OsmAddress, OsmContact } from './types.js';

const AMENITY_TYPE_MAP: Record<string, string> = {
  bar: 'bar',
  pub: 'pub',
  nightclub: 'nightclub',
  theatre: 'theatre',
  arts_centre: 'arts_centre',
  casino: 'casino',
  community_centre: 'community_centre',
  concert_hall: 'concert_hall',
  events_venue: 'events_venue',
};

export interface NormalizeOptions {
  defaultCity: string;
}

export function deriveVenueType(amenity?: string): string {
  if (!amenity) {
    return 'unknown';
  }
  return AMENITY_TYPE_MAP[amenity] ?? amenity;
}

function buildAddress(tags: Record<string, string | undefined>): OsmAddress {
  return {
    housenumber: tags['addr:housenumber'] ?? null,
    street: tags['addr:street'] ?? null,
    postcode: tags['addr:postcode'] ?? null,
    city: tags['addr:city'] ?? null,
    full: tags['addr:full'] ?? null,
  };
}

function buildContact(tags: Record<string, string | undefined>): OsmContact {
  return {
    website: tags.website ?? tags['contact:website'] ?? null,
    phone: tags.phone ?? tags['contact:phone'] ?? null,
    instagram: tags['contact:instagram'] ?? tags.instagram ?? null,
    facebook: tags['contact:facebook'] ?? tags.facebook ?? null,
  };
}

export function formatAddress(address: OsmAddress, fallbackCity: string): string {
  if (address.full) {
    return address.full;
  }
  const parts = [address.housenumber, address.street].filter(Boolean).join(' ').trim();
  const cityParts = [address.postcode, address.city ?? fallbackCity].filter(Boolean).join(' ').trim();
  const combined = [parts, cityParts].filter(Boolean).join(', ');
  return combined || fallbackCity;
}

export function normalizeOsmElement(element: OverpassElement, options: NormalizeOptions): NormalizedOsmVenue | null {
  const tags = element.tags ?? {};
  const name = tags.name ?? tags.operator ?? tags.brand;
  if (!name) {
    return null;
  }

  const coordinates = element.type === 'node'
    ? { lat: element.lat, lon: element.lon }
    : { lat: element.center?.lat, lon: element.center?.lon };

  if (coordinates.lat == null || coordinates.lon == null) {
    return null;
  }

  const address = buildAddress(tags);
  const contact = buildContact(tags);
  const type = deriveVenueType(tags.amenity);
  const city = address.city ?? options.defaultCity;

  return {
    osm_type: element.type,
    osm_id: element.id,
    osm_url: `https://www.openstreetmap.org/${element.type}/${element.id}`,
    name: name.trim(),
    type,
    latitude: roundCoord(coordinates.lat),
    longitude: roundCoord(coordinates.lon),
    adresse: formatAddress(address, city),
    city,
    postcode: address.postcode ?? null,
    address,
    contact,
    opening_hours: tags.opening_hours ?? null,
    capacity: tags.capacity ?? null,
    live_music: tags['live_music'] ?? null,
    osm_tags_raw: { ...tags, derived_type: type, normalized_name: normalizeString(name) },
  };
}
