import pino from 'pino';
import { MongoHelper } from '../utils/mongo-helper';

const COMPLEMENT_TITLE_QUEUE_NAME = 'complement-title';

export class ComplementTitle {
  private readonly mongoDB: MongoHelper;
  private readonly logger: pino.Logger;

  constructor() {
    this.mongoDB = new MongoHelper();
    this.logger = pino({
      name: 'imdb-etl:complement-title',
      level: process.env.LOG_LEVEL || 'info',
    });
  }

  async execute(title: any) {
    const collection = await this.mongoDB.getCollection('catalog', 'titles');
    if (title.titleType === 'movie') {
      const ytsTitle = await this.getYTSTitle(title.imdbId);
      if (!ytsTitle) {
        this.logger.debug(`⛔ Title ${title.imdbId} not found on YTS`);
        return;
      }
      const complementedTitle = {
        ...title,
        ...ytsTitle,
      };
      await collection.updateOne(
        { imdbId: complementedTitle.imdbId },
        { $set: complementedTitle },
      );
      this.logger.info(`🍭 Title ${title.imdbId} complemented`);
    }
  }

  private async getYTSTitle(imdbId: string) {
    const res = await fetch(
      `https://yts.mx/api/v2/movie_details.json?imdb_id=${imdbId}`,
    );
    const json: { data?: { movie?: any } } = await res.json();
    if (json?.data?.movie?.title !== null) {
      return {
        descriptionIntro: json.data.movie.description_intro,
        descriptionFull: json.data.movie.description_full,
        trailers: [
          {
            url: `https://www.youtube.com/watch?v=${json.data.movie.yt_trailer_code}`,
            type: 'youtube',
          },
        ],
        covers: [
          { url: json.data.movie.large_cover_image, size: 'large' },
          { url: json.data.movie.medium_cover_image, size: 'medium' },
          { url: json.data.movie.small_cover_image, size: 'small' },
        ],
        torrents: json.data.movie.torrents.map((torrent: any) => ({
          hash: torrent.hash,
          quality: torrent.quality,
          type: torrent.type,
          url: torrent.url,
          size: torrent.size_bytes,
          seeds: torrent.seeds,
          peers: torrent.peers,
          videoCodec: torrent.video_codec,
          audioChannels: torrent.audio_channels,
        })),
      };
    }
    return null;
  }
}
