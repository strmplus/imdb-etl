import pino from 'pino';
import { MongoHelper } from '../utils/mongo-helper';
import { Queue } from 'bullmq';
import { DOWNLOAD_TORRENT_QUEUE_NAME, REDIS_CONNECTION } from '../constants';

const COMPLEMENT_TITLE_QUEUE_NAME = 'complement-title';

export class ComplementTitle {
  private readonly mongoDB: MongoHelper;
  private readonly logger: pino.Logger;
  private readonly queue: Queue;

  constructor() {
    this.mongoDB = new MongoHelper();
    this.logger = pino({
      name: 'imdb-etl:complement-title',
      level: process.env.LOG_LEVEL || 'info',
    });
    this.queue = new Queue(DOWNLOAD_TORRENT_QUEUE_NAME, {
      connection: REDIS_CONNECTION,
    });
  }

  async execute(title: any) {
    const collection = await this.mongoDB.getCollection('catalog', 'titles');
    const logMetadata = { imdbId: title.imdbId };
    if (title.titleType === 'movie') {
      const ytsTitle = await this.getYTSTitle(title.imdbId);
      if (!ytsTitle) {
        this.logger.debug(logMetadata, `title ${title.imdbId} not found on YTS`);
        return;
      }
      const complementedTitle = {
        ...title,
        ...ytsTitle,
      };
      await collection.updateOne({ imdbId: complementedTitle.imdbId }, { $set: complementedTitle });
      await this.queue.add(DOWNLOAD_TORRENT_QUEUE_NAME, complementedTitle);
      this.logger.info(logMetadata, `title ${title.imdbId} complemented`);
    }
  }

  private async getYTSTitle(imdbId: string) {
    const res = await fetch(`https://yts.mx/api/v2/movie_details.json?imdb_id=${imdbId}`);
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
