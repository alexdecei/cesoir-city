# Cesoir City – BAN Geocoding Tool

This repository contains a Node.js/TypeScript CLI that geocodes venue data with the [BAN API](https://api.gouv.fr/les-api/api-adresse) and synchronises it with the `public.venues` table hosted on Supabase.

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
- `GEOCODE_SCORE_MIN` – minimum acceptable BAN score (default `0.8`)

Outputs are written to `./out` by default (customisable via `--out`).

## CSV Format

The input CSV must contain a header with at least the following columns:

```
nom,adresse,ville[,postcode]
```

A sample file is available in `scripts/sample.csv`.

## Commands

### Geocode only

```bash
npm run geocode -- scripts/input.csv [--continue] [--out ./outdir]
```

- Downloads BAN results with throttling, retry, timeout and caching
- Produces `out/geocoded.jsonl`, `out/ambiguous.csv`, `out/report.json`
- Never writes to Supabase

Use `--continue` to reuse the existing `geocoded.jsonl` cache (no API calls for cached rows).

### Geocode + upsert (default)

```bash
npm run run -- path/to/input.csv [--dry-run] [--continue] [--out ./outdir]
```

- Executes the same geocoding pass
- Inserts or updates matching venues with `barbars=true`
- Produces `out/upserts.csv`, `out/conflicts.csv`, `out/ambiguous.csv`, `out/report.json`

Pass `--dry-run` to simulate Supabase writes while still producing the diff reports. No database mutations are performed in dry-run mode.

### Additional flags

Both commands accept:

- `--score-min <value>` – override the minimum BAN score
- `--concurrency <value>` – override the BAN concurrency limit

## Outputs

All command modes emit artefacts inside the selected output directory:

- `geocoded.jsonl` – local cache of BAN responses (used for `--continue`)
- `ambiguous.csv` – rows requiring manual review (missing fields, no result, low score)
- `upserts.csv` – summary of insert/update/conflict/error outcomes (only for `run` command)
- `conflicts.csv` – detected name collisions with different addresses
- `report.json` – aggregated metrics (API usage, inserts, updates, conflicts, etc.)

## Error handling & safety features

- Exponential backoff on 429/5xx BAN responses (3 attempts, 5s timeout)
- Concurrency throttling via `p-limit`
- Local JSONL cache to avoid repeated API calls across runs
- Address similarity heuristics (normalisation + Levenshtein) prevent overwriting existing venues at a different address
- Progress bar and structured logging via `pino`

## Development

Type checking and builds:

```bash
npm run build
```

The project is written in strict TypeScript and uses native `fetch` (Node 18+).
