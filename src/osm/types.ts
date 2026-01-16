export type OsmElementType = 'node' | 'way' | 'relation';

export interface OsmTags {
  [key: string]: string | undefined;
}

export interface OverpassElement {
  type: OsmElementType;
  id: number;
  lat?: number;
  lon?: number;
  center?: { lat: number; lon: number };
  tags?: OsmTags;
}

export interface OverpassResponse {
  elements: OverpassElement[];
}

export interface AreaCandidate {
  id: number;
  name: string;
  adminLevel?: string;
  population?: number;
  tags: OsmTags;
}

export interface OsmAddress {
  housenumber?: string | null;
  street?: string | null;
  postcode?: string | null;
  city?: string | null;
  full?: string | null;
}

export interface OsmContact {
  website?: string | null;
  phone?: string | null;
  instagram?: string | null;
  facebook?: string | null;
}

export interface NormalizedOsmVenue {
  osm_type: OsmElementType;
  osm_id: number;
  osm_url: string;
  name: string;
  type: string;
  latitude: number;
  longitude: number;
  adresse: string;
  city: string;
  postcode?: string | null;
  address: OsmAddress;
  contact: OsmContact;
  opening_hours?: string | null;
  capacity?: string | null;
  live_music?: string | null;
  osm_tags_raw: OsmTags;
}

export interface OsmEntry {
  status: 'ok' | 'ambiguous' | 'error';
  reason?: string;
  venue?: NormalizedOsmVenue;
  raw?: OverpassElement;
}
