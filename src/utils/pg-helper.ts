import pg from 'pg';

export class PgHelper {
  private readonly client: pg.Pool;

  constructor() {
    this.client = new pg.Pool({ connectionString: process.env.POSTGRES_URL });
  }

  async query(query: string, values: any[] = []) {
    const client = await this.client.connect();
    try {
      const { rows } = await client.query(query, values);
      return rows;
    } finally {
      client.release();
    }
  }
}
