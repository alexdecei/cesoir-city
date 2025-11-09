import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { logger } from './logger.js';

export interface VenueRow {
  id: string;
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  barbars: boolean | null;
}

export interface VenuePayload {
  nom: string;
  adresse: string;
  city: string;
  latitude: number;
  longitude: number;
  barbars: boolean;
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

export async function findVenueByName(nom: string): Promise<VenueRow | null> {
  const supabase = ensureClient();
  const { data, error } = await supabase
    .from('venues')
    .select('*')
    .ilike('nom', nom)
    .limit(1)
    .maybeSingle<VenueRow>();

  if (error) {
    logger.error({ err: error }, 'Supabase findVenueByName failed');
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
  const { error } = await supabase.from('venues').insert(payload);

  if (error) {
    logger.error({ err: error, nom: payload.nom }, 'Supabase insertVenue failed');
    throw error;
  }
}

export async function updateVenue(id: string, payload: Omit<VenuePayload, 'nom'>): Promise<void> {
  const supabase = ensureClient();
  const { error } = await supabase
    .from('venues')
    .update(payload)
    .eq('id', id);

  if (error) {
    logger.error({ err: error, id }, 'Supabase updateVenue failed');
    throw error;
  }
}
