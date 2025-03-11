import { Queue, Worker } from 'bullmq';
import { NormalizeTitles } from './normalize/normalize-titles';
import { FindTitles } from './normalize/find-titles';
import { DownloadAndImportTitles } from './download-and-import/download-and-import-titles';
import pino from 'pino';
import { ComplementTitle } from './complement/complement-title';
import {
  COMPLEMENT_TITLE_QUEUE_NAME,
  COMPLEMENT_TITLES_QUEUE_CONCURRENCY,
  DOWNLOAD_AND_IMPORT_TITLES_QUEUE_NAME,
  FIND_TITLES_QUEUE_NAME,
  NORMALIZE_TITLES_QUEUE_CONCURRENCY,
  NORMALIZE_TITLES_QUEUE_NAME,
  REDIS_CONNECTION,
} from './constants';

const logger = pino({
  name: 'imdb-etl:main',
  level: process.env.LOG_LEVEL || 'info',
});

const downloadAndImportTitles = new DownloadAndImportTitles();
const findTitles = new FindTitles();
const normalizeTitles = new NormalizeTitles();
const complementTitle = new ComplementTitle();

(() => {
  new Queue(DOWNLOAD_AND_IMPORT_TITLES_QUEUE_NAME, {
    connection: REDIS_CONNECTION,
  }).upsertJobScheduler(FIND_TITLES_QUEUE_NAME, {
    pattern: '0 0 1 * *',
  });

  new Queue(FIND_TITLES_QUEUE_NAME, {
    connection: REDIS_CONNECTION,
  }).upsertJobScheduler(FIND_TITLES_QUEUE_NAME, {
    pattern: '0 0 * * 0',
  });

  new Worker(
    DOWNLOAD_AND_IMPORT_TITLES_QUEUE_NAME,
    () => downloadAndImportTitles.execute(),
    {
      connection: REDIS_CONNECTION,
    },
  ).on('failed', (_, error) =>
    logger.error(error, `❌ Download and import titles failed: ${error}`),
  );

  new Worker(FIND_TITLES_QUEUE_NAME, () => findTitles.execute(), {
    connection: REDIS_CONNECTION,
  }).on('failed', (_, error) =>
    logger.error(error, `❌ Finding titles failed: ${error}`),
  );

  new Worker(
    NORMALIZE_TITLES_QUEUE_NAME,
    (job) => normalizeTitles.execute(job.data),
    {
      connection: REDIS_CONNECTION,
      concurrency: NORMALIZE_TITLES_QUEUE_CONCURRENCY,
    },
  ).on('failed', (job, error) =>
    logger.error(
      error,
      `❌ Title ${job.data.tconst} normalization failed: ${error}`,
    ),
  );

  new Worker(
    COMPLEMENT_TITLE_QUEUE_NAME,
    (job) => complementTitle.execute(job.data),
    {
      connection: REDIS_CONNECTION,
      concurrency: COMPLEMENT_TITLES_QUEUE_CONCURRENCY,
    },
  ).on('failed', (job, error) =>
    logger.error(
      error,
      `❌ Title ${job.data.imdbId} complement failed: ${error}`,
    ),
  );

  logger.info('🚀 ETL started');
})();
