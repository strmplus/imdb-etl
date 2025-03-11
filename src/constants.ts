export const REDIS_CONNECTION = {
  url: process.env.REDIS_URL,
};

export const DOWNLOAD_AND_IMPORT_TITLES_QUEUE_NAME =
  'download-and-import-titles';
export const FIND_TITLES_QUEUE_NAME = 'find-titles';
export const NORMALIZE_TITLES_QUEUE_NAME = 'normalize-titles';
export const NORMALIZE_TITLES_QUEUE_CONCURRENCY = 100;
export const COMPLEMENT_TITLE_QUEUE_NAME = 'complement-title';
export const COMPLEMENT_TITLES_QUEUE_CONCURRENCY = 10;
