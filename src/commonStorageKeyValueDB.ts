import * as Buffer from 'buffer'
import { CommonDBCreateOptions, CommonKeyValueDB, KeyValueDBTuple } from '@naturalcycles/db-lib'
import { pMap, StringMap } from '@naturalcycles/js-lib'
import { ReadableTyped, transformMapSimple } from '@naturalcycles/nodejs-lib'
import { CommonStorage, FileEntry } from './commonStorage'

export interface CommonStorageKeyValueDBCfg {
  storage: CommonStorage
  bucketName: string
}

/**
 * CommonKeyValueDB, backed up by a CommonStorage implementation.
 *
 * Each Table is represented as a Folder.
 * Each Item is represented as a File:
 * fileName is ${id} (without extension)
 * file contents is ${v} (Buffer)
 */
export class CommonStorageKeyValueDB implements CommonKeyValueDB {
  constructor(public cfg: CommonStorageKeyValueDBCfg) {}

  async ping(): Promise<void> {
    await this.cfg.storage.ping(this.cfg.bucketName)
  }

  async createTable(_table: string, _opt?: CommonDBCreateOptions): Promise<void> {
    // no-op
  }

  /**
   * Allows to pass `SomeBucket.SomeTable` in `table`, to override a Bucket.
   */
  private getBucketAndPrefix(table: string): { bucketName: string; prefix: string } {
    const [part1, part2] = table.split('.')

    if (part2) {
      return {
        bucketName: part1!,
        prefix: part2,
      }
    }

    // As is
    return {
      bucketName: this.cfg.bucketName,
      prefix: table,
    }
  }

  async deleteByIds(table: string, ids: string[]): Promise<void> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)
    await pMap(ids, async id => {
      await this.cfg.storage.deletePath(bucketName, [prefix, id].join('/'))
    })
  }

  async getByIds(table: string, ids: string[]): Promise<KeyValueDBTuple[]> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)

    const map: StringMap<Buffer> = {}

    await pMap(ids, async id => {
      const buf = await this.cfg.storage.getFile(bucketName, [prefix, id].join('/'))
      if (buf) map[id] = buf
    })

    return ids.map(id => [id, map[id]] as KeyValueDBTuple).filter(t => t[1])
  }

  async saveBatch(table: string, entries: KeyValueDBTuple[]): Promise<void> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)

    await pMap(entries, async ([id, content]) => {
      await this.cfg.storage.saveFile(bucketName, [prefix, id].join('/'), content)
    })
  }

  streamIds(table: string, limit?: number): ReadableTyped<string> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)

    return this.cfg.storage.getFileNamesStream(bucketName, { prefix, limit, fullPaths: false })
  }

  streamValues(table: string, limit?: number): ReadableTyped<Buffer> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)

    return this.cfg.storage
      .getFilesStream(bucketName, { prefix, limit })
      .pipe(transformMapSimple<FileEntry, Buffer>(f => f.content))
  }

  streamEntries(table: string, limit?: number): ReadableTyped<KeyValueDBTuple> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)

    return this.cfg.storage
      .getFilesStream(bucketName, { prefix, limit, fullPaths: false })
      .pipe(
        transformMapSimple<FileEntry, KeyValueDBTuple>(({ filePath, content }) => [
          filePath,
          content,
        ]),
      )
  }

  async count(table: string): Promise<number> {
    const { bucketName, prefix } = this.getBucketAndPrefix(table)

    return (await this.cfg.storage.getFileNames(bucketName, { prefix })).length
  }
}
