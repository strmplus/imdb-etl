import pino from 'pino';
import { MongoHelper } from '../utils/mongo-helper';
import { PgHelper } from '../utils/pg-helper';

export class NormalizeTitles {
  private readonly mongoDB: MongoHelper;
  private readonly pgDB: PgHelper;
  private readonly logger: pino.Logger;

  constructor() {
    this.mongoDB = new MongoHelper();
    this.pgDB = new PgHelper();
    this.logger = pino({
      name: 'imdb-etl:normalize-titles',
      level: process.env.LOG_LEVEL || 'info',
    });
  }

  async execute(title: any) {
    const collection = await this.mongoDB.getCollection('catalog', 'titles');
    const normalizedTitle = {
      imdbId: title.tconst,
      primaryTitle: title.primarytitle,
      originalTitle: title.originaltitle,
      startYear: title.startyear,
      endYear: title.endyear,
      runtimeMinutes: title.runtimeminutes,
      titleType: title.titletype,
      isAdult: title.isadult,
      genres: title.genres?.split(',') ?? [],
      ratings: [],
    };
    normalizedTitle.ratings = await this.getRatings(normalizedTitle.imdbId);
    if (['tvSeries', 'tvMiniSeries'].includes(normalizedTitle.titleType)) {
      const seasons = await this.getSeasons(normalizedTitle.imdbId);
      Object.assign(normalizedTitle, { seasons });
    }
    await collection.updateOne(
      { imdbId: normalizedTitle.imdbId },
      { $set: normalizedTitle },
      { upsert: true },
    );
    this.logger.info(`✅ Title ${normalizedTitle.imdbId} normalized`);
  }

  private async getRatings(imdbId: string) {
    const rows = await this.pgDB.query(
      `
      SELECT 
        averagerating,
        numvotes
      FROM title_ratings 
      WHERE tconst = $1
      AND averagerating IS NOT NULL
      AND numvotes IS NOT NULL
      LIMIT 1
    `,
      [imdbId],
    );
    return rows.map((row) => ({
      source: 'IMDB',
      value: row.averagerating,
      votes: row.numvotes,
    }));
  }

  private async getSeasons(imdbId: string) {
    const rows = await this.pgDB.query(
      `
      SELECT 
        te.tconst,
        te.seasonnumber,
        te.episodenumber,
        tb.primarytitle,
        tb.originaltitle,
        tb.runtimeminutes
      FROM title_episode te
      JOIN title_basics AS tb ON te.tconst = tb.tconst
      WHERE te.parenttconst = $1
      `,
      [imdbId],
    );
    const seasons = rows.reduce((acc, row) => {
      if (!acc[row.seasonnumber]) {
        acc[row.seasonnumber] = [];
      }
      acc[row.seasonnumber].push({
        imdbId: row.tconst,
        episodeNumber: row.episodenumber,
        primaryTitle: row.primarytitle,
        originalTitle: row.originaltitle,
        runtimeMinutes: row.runtimeminutes,
      });
      return acc;
    }, {});
    return seasons;
  }
}
