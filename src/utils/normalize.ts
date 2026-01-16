const STOPWORDS = new Set([
  'le',
  'la',
  'les',
  'l',
  "l'",
  'au',
  'aux',
  'du',
  'de',
  'des',
  "d'",
  'bar',
  'club',
]);

export interface NormalizedName {
  normalized: string;
  tokens: string[];
}

export function normalizeName(input: string): NormalizedName {
  const normalized = input
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}]+/gu, ' ')
    .replace(/\b([ld])\b/g, ' ')
    .trim();

  if (!normalized) {
    return { normalized: '', tokens: [] };
  }

  const tokens = normalized
    .split(/\s+/)
    .map((token) => token.trim())
    .filter((token) => token && !STOPWORDS.has(token));

  return { normalized: tokens.join(' ').trim(), tokens };
}

export function normalizeCity(input: string): string {
  return input
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}]+/gu, ' ')
    .trim();
}

export function normalizeAddress(input: string): string {
  return normalizeCity(input);
}
