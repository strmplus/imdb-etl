import path from 'path';
import pino from 'pino';
import fs from 'node:fs';
import os from 'node:os';
import throttle from 'lodash.throttle';
import WebTorrent from 'webtorrent';
import { LOG_THROTTLE_MS, TORRENT_CONNECT_TIMEOUT_MS, TORRENT_TRACKERS, VIDEO_FILE_EXT } from '../constants';
import { promisify } from 'node:util';
import { EventEmitter } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import slugify from 'slugify';

export class DownloadTorrent {
  private readonly logger: pino.Logger;
  private readonly downloadPath = path.join('/mnt/c/Users/Micael/Downloads', 'torrents');
  private client: WebTorrent.Instance;

  constructor() {
    this.logger = pino({
      name: 'imdb-etl:download-torrent',
      level: process.env.LOG_LEVEL || 'debug',
    });
    if (!fs.existsSync(this.downloadPath)) {
      fs.mkdirSync(this.downloadPath, { recursive: true });
    }
  }

  private async waitForEvent(emitter: EventEmitter, event: string, timeoutMs?: number) {
    if (!timeoutMs) {
      return promisify((cb) => emitter.once(event, cb))();
    }
    return Promise.race([
      promisify((cb) => emitter.once(event, cb))(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`timeout: event '${event}' did not occur`)), timeoutMs),
      ),
    ]);
  }

  async execute(title: any) {
    const WebTorrent = await import('webtorrent');
    if (!this.client) {
      this.client = new WebTorrent.default({ maxConns: 200 });
    }
    const selectedTorrent = title.torrents.find((torrent) => torrent.quality === '1080p');

    const logMetadata = {
      imdbId: title.imdbId,
      hash: selectedTorrent.hash,
      size: this.prettyBytes(selectedTorrent.size),
      quality: selectedTorrent.quality,
      peers: selectedTorrent.peers,
      seeds: selectedTorrent.seeds,
    };

    this.logger.debug(logMetadata, `downloading torrent for ${title.imdbId}`);
    const torrent = this.client.add(selectedTorrent.hash, {
      announceList: [TORRENT_TRACKERS],
      path: this.downloadPath,
    });
    torrent.on(
      'download',
      throttle(() => this.logger.info(logMetadata, this.progressMessage(torrent)), LOG_THROTTLE_MS),
    );

    try {
      await this.waitForEvent(torrent, 'metadata', TORRENT_CONNECT_TIMEOUT_MS);
      await this.waitForEvent(torrent, 'done');
      this.logger.info(logMetadata, `downloadeded for ${title.imdbId}`);
    } catch (error) {
      this.logger.error(logMetadata, `error downloading torrent: ${error.message}`);
    } finally {
      torrent.destroy();
    }
  }

  private prettyBytes(num: number) {
    const units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const neg = num < 0;
    if (neg) num = -num;
    if (num < 1) return `${neg ? '-' : ''}${num} B`;
    const exponent = Math.min(Math.floor(Math.log(num) / Math.log(1000)), units.length - 1);
    const unit = units[exponent];
    num = Number((num / Math.pow(1000, exponent)).toFixed(2));
    return `${neg ? '-' : ''}${num} ${unit}`;
  }

  private progressMessage(torrent: WebTorrent.Torrent) {
    const progress = Math.floor(torrent.progress * 100);
    const downloadSpeed = this.prettyBytes(torrent.downloadSpeed);
    return `downloaded: ${this.prettyBytes(
      torrent.downloaded,
    )}, speed: ${downloadSpeed}, progress: ${progress}%, peers: ${torrent.numPeers}`;
  }
}
