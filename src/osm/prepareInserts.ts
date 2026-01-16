import { promises as fs } from 'fs';
import path from 'path';
import { stripEmpty } from '../export/dbShape.js';

export interface PrepareInsertOptions {
  inputPath: string;
  outDir: string;
}

const ALLOWED_COLUMNS = new Set([
  'nom',
  'latitude',
  'longitude',
  'adresse',
  'image_url',
  'tags',
  'description',
  'event_max',
  'city',
  'programmation',
  'lastchecked',
  'autonome',
  'commentaire',
  'favoris',
  'demarche',
  'plan',
  'instagram',
  'osm_type',
  'osm_id',
  'osm_url',
  'osm_tags_raw',
  'address',
  'contact',
  'osm_venue_type',
  'opening_hours',
  'capacity',
  'live_music',
  'website',
  'phone',
  'facebook',
  'source',
  'osm_last_sync_at',
]);

function parseJsonLines(content: string): Record<string, unknown>[] {
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function escapeSqlValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value.toString() : 'NULL';
  }
  if (typeof value === 'boolean') {
    return value ? 'TRUE' : 'FALSE';
  }
  if (Array.isArray(value)) {
    const escaped = value
      .map((item) => `'${String(item).replace(/'/g, "''")}'`)
      .join(',');
    return `ARRAY[${escaped}]`;
  }
  if (typeof value === 'object') {
    const json = JSON.stringify(value).replace(/'/g, "''");
    return `'${json}'::jsonb`;
  }
  const escaped = String(value).replace(/'/g, "''");
  return `'${escaped}'`;
}

function cleanRecord(record: Record<string, unknown>): Record<string, unknown> {
  const filtered: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(record)) {
    if (!ALLOWED_COLUMNS.has(key)) {
      continue;
    }
    filtered[key] = value;
  }
  return stripEmpty(filtered);
}

function buildInsert(record: Record<string, unknown>): string {
  const entries = Object.entries(record);
  const columns = entries.map(([key]) => key);
  const values = entries.map(([, value]) => escapeSqlValue(value));
  return `INSERT INTO public.venues (${columns.join(', ')}) VALUES (${values.join(', ')});`;
}

export async function runPrepareInserts(options: PrepareInsertOptions): Promise<void> {
  await fs.mkdir(path.resolve(options.outDir), { recursive: true });
  const content = await fs.readFile(options.inputPath, 'utf8');
  const trimmed = content.trim();
  const parsed = trimmed.startsWith('[')
    ? (JSON.parse(trimmed) as Record<string, unknown>[])
    : parseJsonLines(content);

  const cleaned = parsed
    .map((record) => cleanRecord(record))
    .filter((record) => Object.keys(record).length > 0);

  const statements = cleaned.map((record) => buildInsert(record)).join('\n');
  await fs.writeFile(path.join(options.outDir, 'insert_new_venues.sql'), `${statements}\n`, 'utf8');

  const report = {
    total: parsed.length,
    prepared: cleaned.length,
  };

  await fs.writeFile(path.join(options.outDir, 'report.json'), JSON.stringify(report, null, 2), 'utf8');
}
