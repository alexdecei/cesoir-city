alter table public.venues
  add column if not exists osm_type text,
  add column if not exists osm_id bigint,
  add column if not exists osm_url text,
  add column if not exists osm_tags_raw jsonb,
  add column if not exists address jsonb,
  add column if not exists contact jsonb,
  add column if not exists osm_venue_type text,
  add column if not exists opening_hours text,
  add column if not exists capacity integer,
  add column if not exists live_music boolean,
  add column if not exists website text,
  add column if not exists phone text,
  add column if not exists facebook text,
  add column if not exists source text not null default 'manual',
  add column if not exists osm_last_sync_at timestamptz;

create unique index if not exists venues_osm_unique on public.venues(osm_type, osm_id);

alter table public.venues drop constraint if exists venues_nom_key;
create index if not exists venues_nom_idx on public.venues(nom);
