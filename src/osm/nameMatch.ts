import { levenshtein } from '../lib/normalize.js';

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
  'd',
  "d'",
]);

export interface NameMatchResult {
  normalized: string;
  tokens: string[];
}

export interface NameCandidate {
  name: string;
  normalized: string;
}

export interface NameMatchPair {
  nom_osm: string;
  nom_bdd: string;
  normalized_osm: string;
  normalized_bdd: string;
  score: number;
  reason: string;
}

export interface NameMatchAmbiguous {
  nom_osm: string;
  nom_bdd_candidates: Array<{ name: string; score: number }>;
  reason: string;
}

export interface NameMatchOutput {
  matches: NameMatchPair[];
  ambiguous: NameMatchAmbiguous[];
  solo_osm: string[];
  solo_bdd: string[];
}

export function normalizeName(input: string): NameMatchResult {
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

function jaccard(a: string[], b: string[]): number {
  if (a.length === 0 || b.length === 0) {
    return 0;
  }
  const setA = new Set(a);
  const setB = new Set(b);
  const intersection = [...setA].filter((token) => setB.has(token));
  const union = new Set([...setA, ...setB]);
  return intersection.length / union.size;
}

export function similarityScore(a: NameMatchResult, b: NameMatchResult): { score: number; reason: string } {
  if (!a.normalized || !b.normalized) {
    return { score: 0, reason: 'empty' };
  }
  if (a.normalized === b.normalized) {
    return { score: 1, reason: 'exact' };
  }
  if (a.normalized.includes(b.normalized) || b.normalized.includes(a.normalized)) {
    return { score: 0.92, reason: 'substring' };
  }

  const jaccardScore = jaccard(a.tokens, b.tokens);
  const maxLen = Math.max(a.normalized.length, b.normalized.length);
  const distance = levenshtein(a.normalized, b.normalized);
  const levenshteinScore = maxLen > 0 ? 1 - distance / maxLen : 0;

  const score = Math.max(jaccardScore, levenshteinScore);
  const reason = jaccardScore >= levenshteinScore ? 'token_overlap' : 'edit_distance';

  return { score, reason };
}

export function matchNames(osmNames: string[], bddNames: string[]): NameMatchOutput {
  const osmCandidates = osmNames.map((name) => ({
    name,
    ...normalizeName(name),
  }));

  const bddCandidates = bddNames.map((name) => ({
    name,
    ...normalizeName(name),
  }));

  const osmToBdd = new Map<string, Array<{ name: string; score: number; reason: string }>>();
  const bddToOsm = new Map<string, Array<{ name: string; score: number; reason: string }>>();

  for (const osm of osmCandidates) {
    for (const bdd of bddCandidates) {
      const { score, reason } = similarityScore(osm, bdd);
      if (score < 0.82) {
        continue;
      }
      const list = osmToBdd.get(osm.name) ?? [];
      list.push({ name: bdd.name, score, reason });
      osmToBdd.set(osm.name, list);

      const bddList = bddToOsm.get(bdd.name) ?? [];
      bddList.push({ name: osm.name, score, reason });
      bddToOsm.set(bdd.name, bddList);
    }
  }

  const matches: NameMatchPair[] = [];
  const ambiguous: NameMatchAmbiguous[] = [];
  const usedBdd = new Set<string>();
  const usedOsm = new Set<string>();

  for (const osm of osmCandidates) {
    const candidates = (osmToBdd.get(osm.name) ?? []).sort((a, b) => b.score - a.score);
    if (candidates.length === 0) {
      continue;
    }

    const top = candidates[0];
    const bddCandidateList = bddToOsm.get(top.name) ?? [];

    const closeScores = candidates.filter((candidate) => candidate.score >= top.score - 0.03);
    if (closeScores.length > 1 || bddCandidateList.length !== 1) {
      ambiguous.push({
        nom_osm: osm.name,
        nom_bdd_candidates: candidates.map((candidate) => ({
          name: candidate.name,
          score: candidate.score,
        })),
        reason: closeScores.length > 1 ? 'multiple_close_scores' : 'bdd_multiple_matches',
      });
      continue;
    }

    const bddMatch = bddCandidates.find((candidate) => candidate.name === top.name);
    if (!bddMatch) {
      continue;
    }

    matches.push({
      nom_osm: osm.name,
      nom_bdd: bddMatch.name,
      normalized_osm: osm.normalized,
      normalized_bdd: bddMatch.normalized,
      score: top.score,
      reason: top.reason,
    });
    usedBdd.add(bddMatch.name);
    usedOsm.add(osm.name);
  }

  const soloOsm = osmCandidates
    .filter((osm) => !usedOsm.has(osm.name) && !(osmToBdd.get(osm.name)?.length ?? 0))
    .map((osm) => osm.name);
  const soloBdd = bddCandidates
    .filter((bdd) => !usedBdd.has(bdd.name) && !(bddToOsm.get(bdd.name)?.length ?? 0))
    .map((bdd) => bdd.name);

  return {
    matches,
    ambiguous,
    solo_osm: soloOsm,
    solo_bdd: soloBdd,
  };
}
