import { Queue } from 'bullmq';
import { PgHelper } from '../utils/pg-helper';
import pino from 'pino';
import { NORMALIZE_TITLES_QUEUE_NAME, REDIS_CONNECTION } from '../constants';

export class FindTitles {
  private readonly pgDB: PgHelper;
  private readonly queue: Queue;
  private readonly logger: pino.Logger;

  constructor() {
    this.pgDB = new PgHelper();
    this.queue = new Queue(NORMALIZE_TITLES_QUEUE_NAME, {
      connection: REDIS_CONNECTION,
    });
    this.logger = pino({
      name: 'imdb-etl:find-titles',
      level: process.env.LOG_LEVEL || 'info',
    });
  }

  async execute() {
    let offset = 0;
    const limit = 1000;
    let hasMore = false;
    let count = 0;
    do {
      const rows = await this.getTitles(offset, limit);
      hasMore = rows.length > 0;
      if (hasMore) {
        await this.queue.addBulk(
          rows.map((row) => ({ name: NORMALIZE_TITLES_QUEUE_NAME, data: row })),
        );
        this.logger.debug(
          `🔍 ${offset + rows.length} titles found to normalize`,
        );
        offset += limit;
        count += rows.length;
      }
    } while (hasMore);
    this.logger.info(`🔍 ${count} titles found to normalize`);
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
