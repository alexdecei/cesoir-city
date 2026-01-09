alter table public.venues
  add column if not exists osm_type text,
  add column if not exists osm_id bigint,
  add column if not exists osm_url text,
  add column if not exists osm_tags_raw jsonb;

create unique index if not exists venues_osm_unique on public.venues(osm_type, osm_id);
