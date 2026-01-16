# Cesoir City – Venue Seed Tool (BAN + OSM)

This repository contains a Node.js/TypeScript CLI that can:

- **Geocode CSV venue data** with the [BAN API](https://api.gouv.fr/les-api/api-adresse) and synchronise it with the `public.venues` table hosted on Supabase.
- **Mass import nightlife/event venues from OpenStreetMap** using the Overpass API, scoped to an administrative area, and upsert them into Supabase.

## Prerequisites

- Node.js 18+
- npm 9+
- A Supabase project with the `public.venues` schema described in the specification
- A service-role API key (never commit it)

## Setup

```bash
npm install
cp .env.example .env
# Fill SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY in .env
```

Optional environment overrides:

- `GEOCODE_CONCURRENCY` – maximum concurrent BAN requests (default `5`)
- `GEOCODE_SCORE_MIN` – soft minimum BAN score (default `0.8`, rows below are still accepted but logged)
- `OVERPASS_CONCURRENCY` – maximum concurrent Overpass requests (default `1`)
- `OVERPASS_URL` – override the Overpass endpoint (default `https://overpass-api.de/api/interpreter`)

Outputs are written to `./out` by default (customisable via `--out`).

## OSM schema migration (required)

The OSM pipeline stores OSM identity and tags. Apply the migration below before running `osm:run`:

```sql
alter table public.venues
  add column if not exists osm_type text,
  add column if not exists osm_id bigint,
  add column if not exists osm_url text,
  add column if not exists osm_tags_raw jsonb;

create unique index if not exists venues_osm_unique on public.venues(osm_type, osm_id);
```

A copy of this migration is included in `migrations/2024-09-22-add-osm-columns.sql`.

## OSM Commands (Overpass)

### Fetch only (cache + review)

```bash
npm run osm:fetch -- --city "Paris" --country FR --admin-level 8 --out ./outdir [--continue]
```

- Fetches nightlife/event venues from OSM inside the city administrative area.
- Produces `out/osm_fetched.jsonl`, `out/ambiguous.csv`, `out/report.json`.
- Never writes to Supabase.

### Fetch + upsert

```bash
npm run osm:run -- --city "Paris" --country FR --admin-level 8 --out ./outdir [--continue] [--dry-run]
```

- Runs the same Overpass fetch pipeline.
- Upserts into Supabase by `(osm_type, osm_id)`.
- Does not set any extra flags; it only fills OSM identity + location data.
- Produces `out/upserts.csv`, `out/conflicts.csv`, `out/report.json`.

### OSM filters

The Overpass query includes only the following amenity categories:

- `amenity=bar`
- `amenity=pub`
- `amenity=nightclub`
- `amenity=theatre`
- `amenity=arts_centre`
- `amenity=casino`
- `amenity=community_centre`
- `amenity=concert_hall`
- `amenity=events_venue`

## CSV (BAN) Commands

### Geocode only

```bash
npm run geocode -- path/to/input.csv [--continue] [--out ./outdir]
```

- Downloads BAN results with throttling, retry, timeout and caching
- Produces `out/geocoded.jsonl`, `out/report.json`
- Never writes to Supabase

Use `--continue` to reuse the existing `geocoded.jsonl` cache (no API calls for cached rows).

### Geocode + upsert (default)

```bash
npm run run -- path/to/input.csv [--dry-run] [--continue] [--out ./outdir]
```

- Executes the same geocoding pass
- Inserts or updates matching venues based on BAN geocoding
- Produces `out/upserts.csv`, `out/conflicts.csv`, `out/duplicates.csv`, `out/report.json`

Pass `--dry-run` to simulate Supabase writes while still producing the diff reports. No database mutations are performed in dry-run mode.

## Outputs

### OSM

- `osm_fetched.jsonl` – cache of normalized OSM venues (used for `--continue`)
- `ambiguous.csv` – venues missing name/coordinates or ambiguous area matches
- `upserts.csv` – summary of insert/update/conflict/error outcomes (only for `osm:run`)
- `conflicts.csv` – name conflicts where the address/coords disagree
- `report.json` – aggregated metrics (API usage, inserts, updates, conflicts, etc.)

### BAN

- `geocoded.jsonl` – local cache of BAN responses (used for `--continue`)
- `upserts.csv` – summary of insert/update/conflict/error outcomes (only for `run` command)
- `conflicts.csv` – detected name collisions with different addresses
- `duplicates.csv` – rows skipped because another venue in the same city already uses the address and a similar name
- `report.json` – aggregated metrics (API usage, inserts, updates, conflicts, etc.)

## Error handling & safety features

- Exponential backoff on 429/5xx BAN/Overpass responses (3 attempts, 30s timeout)
- Concurrency throttling via `p-limit`
- Local JSONL cache to avoid repeated API calls across runs
- Address similarity heuristics (normalisation + Levenshtein + geo proximity) prevent overwriting existing venues at a different address
- Progress bar and structured logging via `pino`

## Development

Type checking and builds:

```bash
npm run build
```

The project is written in strict TypeScript and uses native `fetch` (Node 18+).
