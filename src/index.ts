import { CloudStorage, CloudStorageCfg } from './cloudStorage'
import { CommonStorage, CommonStorageGetOptions } from './commonStorage'
import { CommonStorageBucket, CommonStorageBucketCfg } from './commonStorageBucket'
import { CommonStorageKeyValueDB, CommonStorageKeyValueDBCfg } from './commonStorageKeyValueDB'
import { InMemoryCommonStorage } from './inMemoryCommonStorage'
import { GCPServiceAccount } from './model'
import { runCommonStorageTest } from './testing/commonStorageTest'

export type {
  CommonStorage,
  CloudStorageCfg,
  CommonStorageGetOptions,
  GCPServiceAccount,
  CommonStorageBucketCfg,
  CommonStorageKeyValueDBCfg,
}

export {
  CloudStorage,
  CommonStorageKeyValueDB,
  CommonStorageBucket,
  InMemoryCommonStorage,
  runCommonStorageTest,
}
