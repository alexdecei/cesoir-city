export function normalizeString(input: string): string {
  return input
    .normalize('NFD')
    .replace(/\p{Diacritic}/gu, '')
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function normalizeAddress(input: string): string {
  return normalizeString(input);
}

export function roundCoord(value: number, precision = 8): number {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

export function levenshtein(a: string, b: string): number {
  if (a === b) {
    return 0;
  }
  if (!a.length) {
    return b.length;
  }
  if (!b.length) {
    return a.length;
  }

  const matrix: number[][] = Array.from({ length: b.length + 1 }, (_, i) => [i]);
  matrix[0] = Array.from({ length: a.length + 1 }, (_, j) => j);

  for (let i = 1; i <= b.length; i += 1) {
    for (let j = 1; j <= a.length; j += 1) {
      const cost = a[j - 1] === b[i - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost,
      );
    }
  }

  return matrix[b.length][a.length];
}

export function isSimilarName(a: string, b: string): boolean {
  const normA = normalizeString(a);
  const normB = normalizeString(b);

  if (!normA || !normB) {
    return false;
  }

  if (normA === normB) {
    return true;
  }

  if (normA.includes(normB) || normB.includes(normA)) {
    return true;
  }

  return levenshtein(normA, normB) <= 2;
}

export interface AddressContext {
  existingCity?: string | null;
  candidateCity?: string | null;
  existingPostcode?: string | null;
  candidatePostcode?: string | null;
  candidateLabel?: string | null;
}

export function isSimilarAddress(
  existingAddress: string,
  candidateAddress: string,
  context: AddressContext = {},
): boolean {
  const existingNorm = normalizeAddress(existingAddress);
  const candidateNorm = normalizeAddress(candidateAddress);

  if (!existingNorm || !candidateNorm) {
    return false;
  }

  const { existingCity, candidateCity, existingPostcode, candidatePostcode, candidateLabel } = context;

  if (existingCity && candidateCity) {
    const normCityExisting = normalizeString(existingCity);
    const normCityCandidate = normalizeString(candidateCity);
    if (normCityExisting && normCityCandidate && normCityExisting !== normCityCandidate) {
      return false;
    }
  }

  if (existingPostcode && candidatePostcode && existingPostcode !== candidatePostcode) {
    return false;
  }

  if (existingNorm === candidateNorm) {
    return true;
  }

  const distance = levenshtein(existingNorm, candidateNorm);
  if (distance <= 4) {
    return true;
  }

  if (candidateLabel) {
    const labelNorm = normalizeAddress(candidateLabel);
    const labelDistance = levenshtein(existingNorm, labelNorm);
    if (labelDistance <= 4) {
      return true;
    }
  }

  const tokensExisting = new Set(existingNorm.split(' '));
  const tokensCandidate = new Set(candidateNorm.split(' '));

  const intersection = [...tokensExisting].filter((token) => tokensCandidate.has(token));
  const minLength = Math.min(tokensExisting.size, tokensCandidate.size);

  if (intersection.length >= Math.max(1, minLength - 1)) {
    return true;
  }

  return false;
}
