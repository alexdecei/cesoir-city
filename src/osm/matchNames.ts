import { promises as fs } from 'fs';
import path from 'path';
import { matchNames, NameMatchPair, NameMatchAmbiguous } from './nameMatch.js';

export interface MatchNamesOptions {
  inputPath: string;
  outDir: string;
}

interface MatchNamesInput {
  nom_osm: string[];
  nom_bdd: string[];
}

function sortByName(list: string[]): string[] {
  return [...list].sort((a, b) => a.localeCompare(b, 'fr', { sensitivity: 'base' }));
}

function sortMatches(list: NameMatchPair[]): NameMatchPair[] {
  return [...list].sort((a, b) => a.nom_osm.localeCompare(b.nom_osm, 'fr', { sensitivity: 'base' }));
}

function sortAmbiguous(list: NameMatchAmbiguous[]): NameMatchAmbiguous[] {
  return [...list].sort((a, b) => a.nom_osm.localeCompare(b.nom_osm, 'fr', { sensitivity: 'base' }));
}

export async function runMatchNames(options: MatchNamesOptions): Promise<void> {
  await fs.mkdir(path.resolve(options.outDir), { recursive: true });
  const content = await fs.readFile(options.inputPath, 'utf8');
  const parsed = JSON.parse(content) as MatchNamesInput;

  if (!Array.isArray(parsed.nom_osm) || !Array.isArray(parsed.nom_bdd)) {
    throw new Error('Input must contain nom_osm and nom_bdd arrays');
  }

  const result = matchNames(parsed.nom_osm, parsed.nom_bdd);

  const matchesSorted = sortMatches(result.matches);
  const ambiguousSorted = sortAmbiguous(result.ambiguous);
  const soloOsmSorted = sortByName(result.solo_osm);
  const soloBddSorted = sortByName(result.solo_bdd);

  await fs.writeFile(path.join(options.outDir, 'probable_matches.json'), JSON.stringify(matchesSorted, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'ambiguous.json'), JSON.stringify(ambiguousSorted, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'solo_osm.json'), JSON.stringify(soloOsmSorted, null, 2), 'utf8');
  await fs.writeFile(path.join(options.outDir, 'solo_bdd.json'), JSON.stringify(soloBddSorted, null, 2), 'utf8');

  const report = {
    osm_total: parsed.nom_osm.length,
    bdd_total: parsed.nom_bdd.length,
    matched: matchesSorted.length,
    ambiguous: ambiguousSorted.length,
    solo_osm: soloOsmSorted.length,
    solo_bdd: soloBddSorted.length,
  };

  await fs.writeFile(path.join(options.outDir, 'report.json'), JSON.stringify(report, null, 2), 'utf8');
}
