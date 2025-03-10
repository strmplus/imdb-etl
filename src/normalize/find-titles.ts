import { log } from '../utils/log';
import { Queue } from 'bullmq';
import { PgHelper } from '../utils/pg-helper';

const NORMALIZE_TITLES_QUEUE_NAME = 'normalize-titles';

export class FindTitles {
  private readonly pgDB: PgHelper;
  private readonly queue: Queue;

  constructor() {
    this.pgDB = new PgHelper();
    this.queue = new Queue(NORMALIZE_TITLES_QUEUE_NAME, { connection: { url: process.env.REDIS_URL } });
  }

  async execute() {
    log('🔍 Searching titles to normalize');
    let offset = 0;
    const limit = 1000;
    let hasMore = false;
    do {
      const rows = await this.getTitles(offset, limit);
      hasMore = rows.length > 0;
      if (hasMore) {
        await this.queue.addBulk(rows.map((row) => ({ name: NORMALIZE_TITLES_QUEUE_NAME, data: row })));
        log(`🔍 ${offset + rows.length} titles found to normalize`);
        offset += limit;
      }
    } while (hasMore);
  }

  async getTitles(offset: number, limit: number) {
    const rows = await this.pgDB.query(`
      SELECT 
        tconst,
        primarytitle,
        originaltitle,
        startyear,
        endyear,
        runtimeminutes,
        genres,
        titletype,
        isadult
      FROM title_basics 
      -- WHERE titletype in ('movie', 'tvSeries', 'tvMovie', 'tvMiniSeries', 'tvSpecial', 'video', 'short') 
      WHERE titletype in ('movie', 'tvSeries', 'tvMiniSeries') 
      ORDER BY tconst DESC
      OFFSET ${offset} LIMIT ${limit}
    `);
    return rows;
  }
}
