import { Queue, Worker } from 'bullmq';
import { log } from './utils/log';
import { NormalizeTitles } from './normalize/normalize-titles';
import { FindTitles } from './normalize/find-titles';
import { DownloadAndImportTitles } from './download-and-import/download-and-import-titles';

const REDIS_CONNECTION = {
  url: process.env.REDIS_URL,
};

const DOWNLOAD_AND_IMPORT_TITLES_QUEUE_NAME = 'download-and-import-titles';
const FIND_TITLES_QUEUE_NAME = 'find-titles';
const NORMALIZE_TITLES_QUEUE_NAME = 'normalize-titles';
const NORMALIZE_TITLES_QUEUE_CONCURRENCY = 1000;

const downloadAndImportTitles = new DownloadAndImportTitles();
const findTitles = new FindTitles();
const normalizeTitles = new NormalizeTitles();

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
  log(`❌ Download and import titles failed: ${error}`),
);

new Worker(FIND_TITLES_QUEUE_NAME, () => findTitles.execute(), {
  connection: REDIS_CONNECTION,
}).on('failed', (_, error) => log(`❌ Finding titles failed: ${error}`));

new Worker(
  NORMALIZE_TITLES_QUEUE_NAME,
  (job) => normalizeTitles.execute(job.data),
  {
    connection: REDIS_CONNECTION,
    concurrency: NORMALIZE_TITLES_QUEUE_CONCURRENCY,
  },
).on('failed', (job, error) =>
  log(`❌ Title ${job.data.tconst} normalization failed: ${error}`),
);
