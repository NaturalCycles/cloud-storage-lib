import { Readable, Writable } from 'node:stream'
import { File, Storage, StorageOptions } from '@google-cloud/storage'
import { _substringAfterLast } from '@naturalcycles/js-lib'
import { ReadableTyped, transformMap, transformMapSimple } from '@naturalcycles/nodejs-lib'
import { CommonStorage, CommonStorageGetOptions, FileEntry } from './commonStorage'
import { GCPServiceAccount } from './model'

export {
  // This is the latest version, to be imported by consumers
  Storage,
  type StorageOptions,
}

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
  /**
   * It's optional, to allow automatic credentials in AppEngine, or GOOGLE_APPLICATION_CREDENTIALS.
   */
  credentials?: GCPServiceAccount
}

export class CloudStorage implements CommonStorage {
  /**
   * Passing the pre-created Storage allows to instantiate it from both
   * GCP Storage and FirebaseStorage.
   */
  constructor(public storage: Storage) {}

  static createFromGCPServiceAccount(cfg: CloudStorageCfg): CloudStorage {
    const storage = new Storage({
      credentials: cfg.credentials,
      // Explicitly passing it here to fix this error:
      // Error: Unable to detect a Project Id in the current environment.
      // To learn more about authentication and Google APIs, visit:
      // https://cloud.google.com/docs/authentication/getting-started
      //     at /root/repo/node_modules/google-auth-library/build/src/auth/googleauth.js:95:31
      projectId: cfg.credentials?.project_id,
    })

    return new CloudStorage(storage)
  }

  static createFromStorageOptions(storageOptions?: StorageOptions): CloudStorage {
    const storage = new Storage(storageOptions)
    return new CloudStorage(storage)
  }

  async ping(bucketName?: string): Promise<void> {
    await this.storage.bucket(bucketName || 'non-existing-for-sure').exists()
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

  async getFileNames(bucketName: string, opt: CommonStorageGetOptions = {}): Promise<string[]> {
    const { prefix, fullPaths = true } = opt
    const [files] = await this.storage.bucket(bucketName).getFiles({
      prefix,
    })

    if (fullPaths) {
      return files.map(f => f.name)
    }

    return files.map(f => _substringAfterLast(f.name, '/'))
  }

  getFileNamesStream(bucketName: string, opt: CommonStorageGetOptions = {}): ReadableTyped<string> {
    const { prefix, fullPaths = true } = opt

    return this.storage
      .bucket(bucketName)
      .getFilesStream({
        prefix,
        maxResults: opt.limit || undefined,
      })
      .pipe(
        transformMapSimple<File, string>(f =>
          fullPaths ? f.name : _substringAfterLast(f.name, '/'),
        ),
      )
  }

  getFilesStream(bucketName: string, opt: CommonStorageGetOptions = {}): ReadableTyped<FileEntry> {
    const { prefix, fullPaths = true } = opt

    return this.storage
      .bucket(bucketName)
      .getFilesStream({
        prefix,
        maxResults: opt.limit || undefined,
      })
      .pipe(
        transformMap<File, FileEntry>(async f => {
          const [content] = await f.download()
          return { filePath: fullPaths ? f.name : _substringAfterLast(f.name, '/'), content }
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
