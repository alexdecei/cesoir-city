import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { logger } from './logger.js';

export interface VenueRow {
  id: string;
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  tags?: string[] | null;
  osm_type?: string | null;
  osm_id?: number | null;
  osm_url?: string | null;
  osm_tags_raw?: Record<string, unknown> | null;
}

const DEFAULT_IMAGE_URL =
  'https://xpsfgrgjltswsnccuujv.supabase.co/storage/v1/object/public/event-covers/bar_defaut.jpg';

export interface VenuePayload {
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  image_url?: string;
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
  source?: string;
  osm_last_sync_at?: string;
}

let client: SupabaseClient | null = null;

function getEnv(key: string): string {
  const value = process.env[key];
  if (!value) {
    throw new Error(`Missing environment variable ${key}`);
  }
  return value;
}

export function ensureClient(): SupabaseClient {
  if (client) {
    return client;
  }

  const supabaseUrl = getEnv('SUPABASE_URL');
  const supabaseKey = getEnv('SUPABASE_SERVICE_ROLE_KEY');

  client = createClient(supabaseUrl, supabaseKey, {
    auth: { persistSession: false },
  });

  return client;
}

function stripUndefined<T extends Record<string, unknown>>(payload: T): T {
  return Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== undefined)) as T;
}

export async function findVenuesByName(nom: string): Promise<VenueRow[]> {
  const supabase = ensureClient();
  const { data, error } = await supabase
    .from('venues')
    .select('*')
    .ilike('nom', nom)
    .limit(10);

  if (error) {
    logger.error({ err: error }, 'Supabase findVenuesByName failed');
    throw error;
  }

  return data ?? [];
}

export async function findVenueByOsm(osmType: string, osmId: number): Promise<VenueRow | null> {
  const supabase = ensureClient();
  const { data, error } = await supabase
    .from('venues')
    .select('*')
    .eq('osm_type', osmType)
    .eq('osm_id', osmId)
    .limit(1)
    .maybeSingle<VenueRow>();

  if (error) {
    logger.error({ err: error, osmType, osmId }, 'Supabase findVenueByOsm failed');
    throw error;
  }

  return data ?? null;
}

export async function findVenueByCity(city: string): Promise<VenueRow[]> {
  const supabase = ensureClient();
  const { data, error } = await supabase
    .from('venues')
    .select('*')
    .ilike('city', city);

  if (error) {
    logger.error({ err: error, city }, 'Supabase findVenueByCity failed');
    throw error;
  }

  return data ?? [];
}

export async function insertVenue(payload: VenuePayload): Promise<void> {
  const supabase = ensureClient();
  const insertPayload = stripUndefined({ ...payload, image_url: payload.image_url ?? DEFAULT_IMAGE_URL });
  const { error } = await supabase
    .from('venues')
    .insert(insertPayload);

  if (error) {
    logger.error({ err: error, nom: payload.nom }, 'Supabase insertVenue failed');
    throw error;
  }
}

export async function updateVenue(id: string, payload: Omit<VenuePayload, 'nom'>): Promise<void> {
  const supabase = ensureClient();
  const { error } = await supabase
    .from('venues')
    .update(stripUndefined(payload))
    .eq('id', id);

  if (error) {
    logger.error({ err: error, id }, 'Supabase updateVenue failed');
    throw error;
  }
}
