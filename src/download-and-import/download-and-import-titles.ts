import fs from 'node:fs';
import { parse as parseCSV, format as formatCSV } from 'fast-csv';
import pg from 'pg';
import zlib from 'node:zlib';
import path from 'node:path';
import { URL } from 'node:url';
import { pipeline } from 'node:stream/promises';
import * as pgCopy from 'pg-copy-streams';
import { DATASETS, type Dataset, type DatasetTransform } from './datasets';
import axios from 'axios';
import { log } from '../utils/log';

const pool = new pg.Pool({ connectionString: process.env.POSTGRES_URL });

const BASE_URL = 'https://datasets.imdbws.com';

export class DownloadAndImportTitles {
  async execute() {
    try {
      const dataDir = path.join('/tmp', 'imdb-datasets');

      if (!fs.existsSync(dataDir)) {
        fs.mkdirSync(dataDir, { recursive: true });
      }

      for (const dataset of DATASETS) {
        const fileUrl = new URL(BASE_URL);
        fileUrl.pathname = dataset.file;
        const zipFilePath = path.join(dataDir, dataset.file);
        const csvFilePath = zipFilePath.replace('.gz', '');
        await this.downloadFile(fileUrl, zipFilePath);
        await this.extractFile(zipFilePath, csvFilePath, dataset.transform);
        await this.importFileToDatabase(dataset, csvFilePath);
        fs.rmSync(zipFilePath);
        fs.rmSync(csvFilePath);
      }

      log('🎉 ETL process completed successfully!');
    } catch (error) {
      log(`❌ ETL process failed: ${error}`);
      throw error;
    } finally {
      await pool.end();
      log('💾 Database connection closed');
    }
  }

  private async downloadFile(fileUrl: URL, filePath: string) {
    log(`📡 Starting download ${fileUrl}`);
    const { data } = await axios.get(fileUrl.toString(), {
      responseType: 'stream',
    });
    await pipeline(data, fs.createWriteStream(filePath));
    log(`📡 Download completed ${fileUrl}`);
  }

  private async extractFile(
    zipPath: string,
    tsvPath: string,
    transform?: DatasetTransform,
  ) {
    log(`📦 Starting extraction ${zipPath}`);
    await pipeline(
      fs.createReadStream(zipPath),
      zlib.createUnzip(),
      parseCSV({
        delimiter: '\t',
        quote: null,
        headers: true,
        ignoreEmpty: true,
      }),
      formatCSV({ delimiter: '\t' }).transform((row) =>
        transform ? transform(row) : row,
      ),
      fs.createWriteStream(tsvPath, { flags: 'w' }),
    );
    log(`📦 Extraction completed ${zipPath}`);
  }

  private async importFileToDatabase(dataset: Dataset, tsvPath: string) {
    log(`💾 Starting data import ${dataset.name}`);
    const db = await pool.connect();
    try {
      await db.query(
        `CREATE TABLE IF NOT EXISTS ${dataset.name}(${dataset.columns.join(
          ',',
        )});`,
      );
      await db.query(`TRUNCATE ${dataset.name};`);
      for (const index of dataset.indexes ?? []) {
        await db.query(
          `CREATE INDEX IF NOT EXISTS idx_${dataset.name}_${index} ON ${dataset.name}(${index});`,
        );
      }
      const ingestStream = db.query(
        pgCopy.from(`COPY ${dataset.name} FROM STDIN`),
      );
      const sourceStream = fs.createReadStream(tsvPath, {
        highWaterMark: 64 * 1024,
      }); // 64KB chunks for better performance
      await pipeline(sourceStream, ingestStream);
    } finally {
      db.release();
    }
    log(`💾 Data import completed ${dataset.name}`);
  }
}
