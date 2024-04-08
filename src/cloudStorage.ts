import { Readable, Writable } from 'node:stream'
import { File, Storage, StorageOptions } from '@google-cloud/storage'
import {
  _assert,
  _chunk,
  _since,
  _substringAfterLast,
  localTime,
  LocalTimeInput,
  pMap,
  SKIP,
} from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonStorage, CommonStorageGetOptions, FileEntry } from './commonStorage'
import { GCPServiceAccount } from './model'

export {
  // This is the latest version, to be imported by consumers
  Storage,
  type StorageOptions,
}

const MAX_RECURSION_DEPTH = 10
const BATCH_SIZE = 32

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

/**
 * CloudStorage implementation of CommonStorage API.
 *
 * API: https://googleapis.dev/nodejs/storage/latest/index.html
 */
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
    await this.deletePaths(bucketName, [prefix])
  }

  async deletePaths(bucketName: string, prefixes: string[]): Promise<void> {
    const bucket = this.storage.bucket(bucketName)

    await pMap(prefixes, async prefix => {
      await bucket.deleteFiles({
        prefix,
        // to keep going in case error occurs, similar to THROW_AGGREGATED
        force: true,
      })
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
      // Paths that end with `/` are "folders", which are "virtual" in CloudStorage
      // It doesn't make sense to return or do anything with them
      return files.map(f => f.name).filter(s => !s.endsWith('/'))
    }

    return files.map(f => _substringAfterLast(f.name, '/')).filter(Boolean)
  }

  getFileNamesStream(bucketName: string, opt: CommonStorageGetOptions = {}): ReadableTyped<string> {
    const { prefix, fullPaths = true } = opt

    return (
      this.storage.bucket(bucketName).getFilesStream({
        prefix,
        maxResults: opt.limit || undefined,
      }) as ReadableTyped<File>
    ).flatMap(f => {
      const r = this.normalizeFilename(f.name, fullPaths)
      if (r === SKIP) return []
      return [r]
    })
  }

  getFilesStream(bucketName: string, opt: CommonStorageGetOptions = {}): ReadableTyped<FileEntry> {
    const { prefix, fullPaths = true } = opt

    return (
      this.storage.bucket(bucketName).getFilesStream({
        prefix,
        maxResults: opt.limit || undefined,
      }) as ReadableTyped<File>
    ).flatMap(
      async f => {
        const filePath = this.normalizeFilename(f.name, fullPaths)
        if (filePath === SKIP) return []

        const [content] = await f.download()
        return [{ filePath, content }] as FileEntry[]
      },
      {
        concurrency: 16,
      },
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

  async uploadFile(
    localFilePath: string,
    bucketName: string,
    bucketFilePath: string,
  ): Promise<void> {
    await this.storage.bucket(bucketName).upload(localFilePath, {
      destination: bucketFilePath,
    })
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

  async movePath(
    fromBucket: string,
    fromPrefix: string,
    toPrefix: string,
    toBucket?: string,
  ): Promise<void> {
    _assert(fromPrefix.endsWith('/'), 'fromPrefix should end with `/`')
    _assert(toPrefix.endsWith('/'), 'toPrefix should end with `/`')

    await this.storage
      .bucket(fromBucket)
      .getFilesStream({
        prefix: fromPrefix,
      })
      .forEach(async file => {
        const { name } = file
        const newName = toPrefix + name.slice(fromPrefix.length)
        await file.move(this.storage.bucket(toBucket || fromBucket).file(newName))
      })
  }

  async deleteFiles(
    bucketName: string,
    filePaths: string[],
    concurrency: number = 8,
  ): Promise<void> {
    await pMap(
      filePaths,
      async filePath => {
        await this.storage.bucket(bucketName).file(filePath).delete()
      },
      {
        concurrency,
      },
    )
  }

  async combine(
    bucketName: string,
    filePaths: string[],
    toPath: string,
    toBucket?: string,
    recursionDepth: number = 0,
  ): Promise<void> {
    if (recursionDepth > MAX_RECURSION_DEPTH) {
      throw new Error(`Too many recursive calls: ${recursionDepth} (max: ${MAX_RECURSION_DEPTH})`)
    }
    // console.log("waiting 2 minutes")
    // await new Promise(resolve => setTimeout(resolve, 120000)); // Sleep for 2 minutes

    console.log(
      `[${recursionDepth}] Will compose ${filePaths.length} files, by batches of ${BATCH_SIZE}`,
    )
    const intermediateFiles: string[] = []
    if (filePaths.length <= BATCH_SIZE) {
      await this.storage
        .bucket(bucketName)
        .combine(filePaths, this.storage.bucket(toBucket || bucketName).file(toPath))
      console.log(`[${recursionDepth}] Composed into ${toPath}!`)

      await this.deleteFiles(bucketName, filePaths)
      return
    }
    const started = Date.now()
    await pMap(
      _chunk(filePaths, BATCH_SIZE),
      async (fileBatch: string[], i: number) => {
        console.log(`[${recursionDepth}] Composing batch ${i + 1}...`)
        const intermediateFile = `temp_${recursionDepth}_${i}`
        await this.storage
          .bucket(bucketName)
          .combine(fileBatch, this.storage.bucket(toBucket || bucketName).file(intermediateFile))
        intermediateFiles.push(intermediateFile)
        await this.deleteFiles(bucketName, fileBatch)
      },
      {
        concurrency: 8,
      },
    )
    console.log(
      `[${recursionDepth}] Batch composed into ${intermediateFiles.length} files, in ${_since(started)}`,
    )

    await this.combine(
      toBucket || bucketName,
      intermediateFiles,
      toPath,
      toBucket,
      recursionDepth + 1,
    )
  }

  async combineAll(
    bucketName: string,
    prefix: string,
    toPath: string,
    toBucket?: string,
  ): Promise<void> {
    const filePaths = await this.getFileNames(bucketName, { prefix })
    await this.combine(bucketName, filePaths, toPath, toBucket)
  }

  /**
   * Acquires a "signed url", which allows bearer to use it to download ('read') the file.
   *
   * expires: 'v4' supports maximum duration of 7 days from now.
   *
   * @experimental - not tested yet
   */
  async getSignedUrl(
    bucketName: string,
    filePath: string,
    expires: LocalTimeInput,
  ): Promise<string> {
    const [url] = await this.storage
      .bucket(bucketName)
      .file(filePath)
      .getSignedUrl({
        action: 'read',
        version: 'v4',
        expires: localTime(expires).unixMillis(),
      })

    return url
  }

  /**
   * Returns SKIP if fileName is a folder.
   * If !fullPaths - strip away the folder prefix.
   */
  private normalizeFilename(fileName: string, fullPaths: boolean): string | typeof SKIP {
    if (fullPaths) {
      if (fileName.endsWith('/')) return SKIP // skip folders
      return fileName
    }

    fileName = _substringAfterLast(fileName, '/')
    return fileName || SKIP // skip folders
  }
}
