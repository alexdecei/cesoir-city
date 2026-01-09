import { promises as fs } from 'fs';
import path from 'path';
import { stringify } from 'csv-stringify/sync';

export async function writeCsv<T extends Record<string, unknown>>(
  filePath: string,
  records: T[],
  columns: string[],
): Promise<void> {
  const data = stringify(records, { header: true, columns });
  await fs.mkdir(path.dirname(path.resolve(filePath)), { recursive: true });
  await fs.writeFile(filePath, data, 'utf8');
}
