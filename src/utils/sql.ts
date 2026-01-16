export function escapeSqlValue(value: unknown): string {
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

export function buildUpsertSql(table: string, record: Record<string, unknown>): string {
  const entries = Object.entries(record);
  const columns = entries.map(([key]) => key);
  const values = entries.map(([, value]) => escapeSqlValue(value));
  const insert = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${values.join(', ')})`;

  if (record.osm_type && record.osm_id) {
    const updates = entries
      .filter(([key]) => key !== 'osm_type' && key !== 'osm_id')
      .map(([key, value]) => `${key} = ${escapeSqlValue(value)}`)
      .join(', ');
    return `${insert} ON CONFLICT (osm_type, osm_id) DO UPDATE SET ${updates};`;
  }

  return `${insert};`;
}
