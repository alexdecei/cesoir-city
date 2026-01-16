import { writeCsv } from '../lib/csv.js';
import { fetchOverpass } from './overpass.js';
import { AreaCandidate, OverpassResponse } from './types.js';

export interface AreaSearchOptions {
  city: string;
  country?: string;
  adminLevel: number;
}

export interface AreaSearchResult {
  selected: AreaCandidate | null;
  candidates: AreaCandidate[];
  ambiguous: boolean;
  areaId?: number;
}

function parsePopulation(value?: string): number | undefined {
  if (!value) {
    return undefined;
  }
  const numeric = Number(value.replace(/[^0-9]/g, ''));
  return Number.isFinite(numeric) ? numeric : undefined;
}

function buildCountryAreaClause(country: string | undefined): string {
  if (!country) {
    return '';
  }
  return `area["ISO3166-1"="${country}"]["boundary"="administrative"]["admin_level"="2"]->.country;`;
}

export function buildAreaQuery({ city, country, adminLevel }: AreaSearchOptions): string {
  const countryClause = buildCountryAreaClause(country);
  const areaFilter = country ? '(area.country)' : '';
  return `
[out:json][timeout:25];
${countryClause}
relation["boundary"="administrative"]["admin_level"="${adminLevel}"]["name"="${city}"]${areaFilter};
out tags;
`;
}

export async function findAdminArea(options: AreaSearchOptions): Promise<AreaSearchResult> {
  const query = buildAreaQuery(options);
  const response = await fetchOverpass(query);

  const candidates: AreaCandidate[] = response.elements
    .filter((element) => element.type === 'relation' && element.tags?.name)
    .map((element) => ({
      id: element.id,
      name: element.tags?.name ?? 'unknown',
      adminLevel: element.tags?.admin_level,
      population: parsePopulation(element.tags?.population),
      tags: element.tags ?? {},
    }));

  if (candidates.length === 0) {
    return { selected: null, candidates: [], ambiguous: false };
  }

  const normalizedTarget = options.city.toLowerCase();
  const exactMatches = candidates.filter((candidate) => candidate.name.toLowerCase() === normalizedTarget);

  const pool = exactMatches.length > 0 ? exactMatches : candidates;
  const sorted = [...pool].sort((a, b) => (b.population ?? 0) - (a.population ?? 0));
  const selected = sorted[0] ?? null;

  const ambiguous = pool.length > 1;
  const areaId = selected ? selected.id + 3_600_000_000 : undefined;

  return { selected, candidates: pool, ambiguous, areaId };
}

export interface AmbiguousAreaRecord extends Record<string, unknown> {
  name: string;
  relation_id: number;
  admin_level?: string;
  population?: number;
}

export async function writeAmbiguousAreas(
  outPath: string,
  candidates: AreaCandidate[],
): Promise<void> {
  const records: AmbiguousAreaRecord[] = candidates.map((candidate) => ({
    name: candidate.name,
    relation_id: candidate.id,
    admin_level: candidate.adminLevel,
    population: candidate.population,
  }));

  await writeCsv(outPath, records, ['name', 'relation_id', 'admin_level', 'population']);
}
