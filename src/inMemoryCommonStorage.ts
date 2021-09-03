import { Readable, Writable } from 'stream'
import { StringMap } from '@naturalcycles/js-lib'
import { ReadableTyped } from '@naturalcycles/nodejs-lib'
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

  async getFileNames(bucketName: string, prefix: string): Promise<string[]> {
    return Object.keys(this.data[bucketName] || {}).filter(filePath => filePath.startsWith(prefix))
  }

  getFileNamesStream(
    bucketName: string,
    prefix: string,
    opt: CommonStorageGetOptions = {},
  ): ReadableTyped<string> {
    return Readable.from(
      Object.keys(this.data[bucketName] || {})
        .filter(filePath => filePath.startsWith(prefix))
        .slice(0, opt.limit),
    )
  }

  getFilesStream(
    bucketName: string,
    prefix: string,
    opt: CommonStorageGetOptions = {},
  ): ReadableTyped<FileEntry> {
    return Readable.from(
      Object.entries(this.data[bucketName] || {})
        .map(([filePath, content]) => ({ filePath, content }))
        .filter(f => f.filePath.startsWith(prefix))
        .slice(0, opt.limit),
    )
  }

  getFileReadStream(bucketName: string, filePath: string): Readable {
    return Readable.from(this.data[bucketName]![filePath]!)
  }

  getFileWriteStream(_bucketName: string, _filePath: string): Writable {
    throw new Error('Method not implemented.')
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
}
