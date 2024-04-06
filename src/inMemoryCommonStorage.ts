import { Readable, Writable } from 'node:stream'
import { _stringMapEntries, _substringAfterLast, StringMap } from '@naturalcycles/js-lib'
import { fs2, ReadableTyped } from '@naturalcycles/nodejs-lib'
import { CommonStorage, CommonStorageGetOptions, FileEntry } from './commonStorage'

export class InMemoryCommonStorage implements CommonStorage {
  /**
   * data[bucketName][filePath] = Buffer
   */
  data: StringMap<StringMap<Buffer>> = {}

  publicMap: StringMap<StringMap<boolean>> = {}

  async ping(): Promise<void> {}

  async getBucketNames(): Promise<string[]> {
    return Object.keys(this.data)
  }

  getBucketNamesStream(): ReadableTyped<string> {
    return Readable.from(Object.keys(this.data))
  }

  async fileExists(bucketName: string, filePath: string): Promise<boolean> {
    return !!this.data[bucketName]?.[filePath]
  }

  async getFile(bucketName: string, filePath: string): Promise<Buffer | null> {
    return this.data[bucketName]?.[filePath] || null
  }

  async saveFile(bucketName: string, filePath: string, content: Buffer): Promise<void> {
    this.data[bucketName] ||= {}
    this.data[bucketName]![filePath] = content
  }

  async deletePath(bucketName: string, prefix: string): Promise<void> {
    Object.keys(this.data[bucketName] || {}).forEach(filePath => {
      if (filePath.startsWith(prefix)) {
        delete this.data[bucketName]![filePath]
      }
    })
  }

  async getFileNames(bucketName: string, opt: CommonStorageGetOptions = {}): Promise<string[]> {
    const { prefix = '', fullPaths = true } = opt
    return Object.keys(this.data[bucketName] || {})
      .filter(filePath => filePath.startsWith(prefix))
      .map(f => (fullPaths ? f : _substringAfterLast(f, '/')))
  }

  getFileNamesStream(bucketName: string, opt: CommonStorageGetOptions = {}): ReadableTyped<string> {
    const { prefix = '', fullPaths = true } = opt

    return Readable.from(
      Object.keys(this.data[bucketName] || {})
        .filter(filePath => filePath.startsWith(prefix))
        .slice(0, opt.limit)
        .map(n => (fullPaths ? n : _substringAfterLast(n, '/'))),
    )
  }

  getFilesStream(bucketName: string, opt: CommonStorageGetOptions = {}): ReadableTyped<FileEntry> {
    const { prefix = '', fullPaths = true } = opt

    return Readable.from(
      Object.entries(this.data[bucketName] || {})
        .map(([filePath, content]) => ({
          filePath,
          content,
        }))
        .filter(f => f.filePath.startsWith(prefix))
        .slice(0, opt.limit)
        .map(f => (fullPaths ? f : { ...f, filePath: _substringAfterLast(f.filePath, '/') })),
    )
  }

  getFileReadStream(bucketName: string, filePath: string): Readable {
    return Readable.from(this.data[bucketName]![filePath]!)
  }

  getFileWriteStream(_bucketName: string, _filePath: string): Writable {
    throw new Error('Method not implemented.')
  }

  async uploadFile(
    localFilePath: string,
    bucketName: string,
    bucketFilePath: string,
  ): Promise<void> {
    this.data[bucketName] ||= {}
    this.data[bucketName]![bucketFilePath] = await fs2.readBufferAsync(localFilePath)
  }

  async setFileVisibility(bucketName: string, filePath: string, isPublic: boolean): Promise<void> {
    this.publicMap[bucketName] ||= {}
    this.publicMap[bucketName]![filePath] = isPublic
  }

  async getFileVisibility(bucketName: string, filePath: string): Promise<boolean> {
    return !!this.publicMap[bucketName]?.[filePath]
  }

  async copyFile(
    fromBucket: string,
    fromPath: string,
    toPath: string,
    toBucket?: string,
  ): Promise<void> {
    const tob = toBucket || fromBucket
    this.data[fromBucket] ||= {}
    this.data[tob] ||= {}
    this.data[tob]![toPath] = this.data[fromBucket]![fromPath]
  }

  async moveFile(
    fromBucket: string,
    fromPath: string,
    toPath: string,
    toBucket?: string,
  ): Promise<void> {
    const tob = toBucket || fromBucket
    this.data[fromBucket] ||= {}
    this.data[tob] ||= {}
    this.data[tob]![toPath] = this.data[fromBucket]![fromPath]
    delete this.data[fromBucket]![fromPath]
  }

  async movePath(
    fromBucket: string,
    fromPrefix: string,
    toPrefix: string,
    toBucket?: string,
  ): Promise<void> {
    const tob = toBucket || fromBucket
    this.data[fromBucket] ||= {}
    this.data[tob] ||= {}

    _stringMapEntries(this.data[fromBucket]!).forEach(([filePath, v]) => {
      if (!filePath.startsWith(fromPrefix)) return
      this.data[tob]![toPrefix + filePath.slice(fromPrefix.length)] = v
      delete this.data[fromBucket]![filePath]
    })
  }
}
