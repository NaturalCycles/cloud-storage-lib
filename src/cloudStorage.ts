import * as Buffer from 'buffer'
import { Readable, Writable } from 'stream'
import { Bucket, File, Storage } from '@google-cloud/storage'
import { ReadableTyped, transformMap, transformMapSimple } from '@naturalcycles/nodejs-lib'
import { CommonStorage, CommonStorageGetOptions, FileEntry } from './commonStorage'
import { GCPServiceAccount } from './model'

/**
 * This object is intentionally made to NOT extend StorageOptions,
 * because StorageOptions is complicated and provides just too many ways
 * to configure credentials.
 *
 * Here we define the minimum simple set of credentials needed.
 * All of these properties are available from the "service account" json file
 * (either personal one or non-personal).
 */
export interface CloudStorageCfg {
  credentials: GCPServiceAccount
}

export class CloudStorage implements CommonStorage {
  constructor(public cfg: CloudStorageCfg) {
    this.storage = new Storage({
      credentials: cfg.credentials,
      // Explicitly passing it here to fix this error:
      // Error: Unable to detect a Project Id in the current environment.
      // To learn more about authentication and Google APIs, visit:
      // https://cloud.google.com/docs/authentication/getting-started
      //     at /root/repo/node_modules/google-auth-library/build/src/auth/googleauth.js:95:31
      projectId: cfg.credentials.project_id,
    })
  }

  storage: Storage

  // async createBucket(bucketName: string): Promise<void> {
  //   const bucket = await this.storage.createBucket(bucketName)
  //   console.log(bucket) // debugging
  // }

  async ping(bucketName?: string): Promise<void> {
    await this.storage.bucket(bucketName || 'non-existing-for-sure').exists()
  }

  async getBucketNames(opt: CommonStorageGetOptions = {}): Promise<string[]> {
    const [buckets] = await this.storage.getBuckets({
      maxResults: opt.limit,
    })

    return buckets.map(b => b.name)
  }

  getBucketNamesStream(): ReadableTyped<string> {
    return this.storage.getBucketsStream().pipe(transformMapSimple<Bucket, string>(b => b.name))
  }

  async deletePath(bucketName: string, prefix: string): Promise<void> {
    await this.storage.bucket(bucketName).deleteFiles({
      prefix,
      // to keep going in case error occurs, similar to THROW_AGGREGATED
      force: true,
    })
  }

  async fileExists(bucketName: string, filePath: string): Promise<boolean> {
    const [exists] = await this.storage.bucket(bucketName).file(filePath).exists()
    return exists
  }

  async getFileNames(bucketName: string, prefix: string): Promise<string[]> {
    const [files] = await this.storage.bucket(bucketName).getFiles({
      prefix,
    })
    return files.map(f => f.name)
  }

  getFileNamesStream(
    bucketName: string,
    prefix: string,
    opt: CommonStorageGetOptions = {},
  ): ReadableTyped<string> {
    return this.storage
      .bucket(bucketName)
      .getFilesStream({
        prefix,
        maxResults: opt.limit,
      })
      .pipe(transformMapSimple<File, string>(f => f.name))
  }

  getFilesStream(
    bucketName: string,
    prefix: string,
    opt: CommonStorageGetOptions = {},
  ): ReadableTyped<FileEntry> {
    return this.storage
      .bucket(bucketName)
      .getFilesStream({
        prefix,
        maxResults: opt.limit,
      })
      .pipe(
        transformMap<File, FileEntry>(async f => {
          const [content] = await f.download()
          return { filePath: f.name, content }
        }),
      )
  }

  async getFile(bucketName: string, filePath: string): Promise<Buffer | null> {
    const [buf] = await this.storage
      .bucket(bucketName)
      .file(filePath)
      .download()
      .catch(err => {
        if (err?.code === 404) return [null] // file not found
        throw err // rethrow otherwise
      })

    return buf
  }

  /**
   * Returns a Readable that is NOT object mode,
   * so you can e.g pipe it to fs.createWriteStream()
   */
  getFileReadStream(bucketName: string, filePath: string): Readable {
    return this.storage.bucket(bucketName).file(filePath).createReadStream()
  }

  async saveFile(bucketName: string, filePath: string, content: Buffer): Promise<void> {
    await this.storage.bucket(bucketName).file(filePath).save(content)
  }

  getFileWriteStream(bucketName: string, filePath: string): Writable {
    return this.storage.bucket(bucketName).file(filePath).createWriteStream()
  }

  async setFileVisibility(bucketName: string, filePath: string, isPublic: boolean): Promise<void> {
    await this.storage.bucket(bucketName).file(filePath)[isPublic ? 'makePublic' : 'makePrivate']()
  }

  async getFileVisibility(bucketName: string, filePath: string): Promise<boolean> {
    const [isPublic] = await this.storage.bucket(bucketName).file(filePath).isPublic()
    return isPublic
  }

  async copyFile(
    fromBucket: string,
    fromPath: string,
    toPath: string,
    toBucket?: string,
  ): Promise<void> {
    await this.storage
      .bucket(fromBucket)
      .file(fromPath)
      .copy(this.storage.bucket(toBucket || fromBucket).file(toPath))
  }

  async moveFile(
    fromBucket: string,
    fromPath: string,
    toPath: string,
    toBucket?: string,
  ): Promise<void> {
    await this.storage
      .bucket(fromBucket)
      .file(fromPath)
      .move(this.storage.bucket(toBucket || fromBucket).file(toPath))
  }
}
