import { Pool } from "pg";

declare global {
  // eslint-disable-next-line no-var
  var __controlTowerPgPool: Pool | undefined;
}

function createPool() {
  const connectionString = String(process.env.DATABASE_URL || "").trim();
  if (!connectionString) {
    throw new Error("Missing DATABASE_URL env var");
  }
  return new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 10_000,
  });
}

export function getDbPool() {
  if (!global.__controlTowerPgPool) {
    global.__controlTowerPgPool = createPool();
  }
  return global.__controlTowerPgPool;
}

export async function pingDb() {
  const pool = getDbPool();
  const result = await pool.query<{
    now_utc: string;
    db_name: string;
    db_user: string;
  }>(
    `
      select
        now() at time zone 'utc' as now_utc,
        current_database() as db_name,
        current_user as db_user
    `,
  );
  return result.rows[0] || null;
}
