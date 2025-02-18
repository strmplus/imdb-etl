import fs from 'node:fs';
import fcsv from 'fast-csv';
import pg from 'pg';
import zlib from 'node:zlib';
import path from 'node:path';
import { URL } from 'node:url';
import { pipeline } from 'node:stream/promises';
import * as pgCopy from 'pg-copy-streams';
import { DATASETS, type Dataset, type DatasetTransform } from './datasets.ts';
import axios from 'axios';

const PG_CONFIG = {
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: Number(process.env.PG_PORT),
};

const pool = new pg.Pool(PG_CONFIG);

const BASE_URL = 'https://datasets.imdbws.com';

function log(message: string) {
  console.log(`[${new Date().toISOString()}] ${message}`);
}

async function downloadFile(fileUrl: URL, filePath: string) {
  log(`📁 Starting download ${fileUrl}`);
  const { data } = await axios.get(fileUrl.toString(), { responseType: 'stream' });
  await pipeline(data, fs.createWriteStream(filePath));
  log(`📁 Download completed ${fileUrl}`);
}

async function extractFile(zipPath: string, tsvPath: string, transform?: DatasetTransform) {
  log(`📦 Starting extraction ${zipPath}`);
  await pipeline(
    fs.createReadStream(zipPath),
    zlib.createUnzip(),
    fcsv.parse({ delimiter: '\t', quote: null, headers: true, ignoreEmpty: true }),
    fcsv.format({ delimiter: '\t' }).transform((row) => (transform ? transform(row) : row)),
    fs.createWriteStream(tsvPath, { flags: 'w' }),
  );
  log(`📦 Extraction completed ${zipPath}`);
}

async function importFileToDatabase(dataset: Dataset, tsvPath: string) {
  log(`🎲 Starting data import ${dataset.name}`);
  const db = await pool.connect();
  try {
    await db.query(`CREATE TABLE IF NOT EXISTS ${dataset.name}(${dataset.columns.join(',')});`);
    await db.query(`TRUNCATE ${dataset.name};`);
    for (const index of dataset.indexes ?? []) {
      await db.query(`CREATE INDEX IF NOT EXISTS idx_${dataset.name}_${index} ON ${dataset.name}(${index});`);
    }
    const ingestStream = db.query(pgCopy.from(`COPY ${dataset.name} FROM STDIN`));
    const sourceStream = fs.createReadStream(tsvPath, { highWaterMark: 64 * 1024 }); // 64KB chunks for better performance
    await pipeline(sourceStream, ingestStream);
  } finally {
    db.release();
  }
  log(`🎲 Data import completed ${dataset.name}`);
}

async function runETL() {
  const dataDir = path.join('/tmp', 'imdb-datasets');

  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  await Promise.all(
    DATASETS.map(async (dataset) => {
      const fileUrl = new URL(BASE_URL);
      fileUrl.pathname = dataset.file;
      const zipFilePath = path.join(dataDir, dataset.file);
      const csvFilePath = zipFilePath.replace('.gz', '');
      await downloadFile(fileUrl, zipFilePath);
      await extractFile(zipFilePath, csvFilePath, dataset.transform);
      await importFileToDatabase(dataset, csvFilePath);
    }),
  );

  fs.rmdirSync(dataDir, { recursive: true });

  log('🎉 ETL process completed successfully!');
}

runETL().catch((error) => log(`❌ ETL process failed: ${error}`));
