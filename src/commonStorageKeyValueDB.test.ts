import { runCommonKeyValueDBTest } from '@naturalcycles/db-lib/dist/testing'
import { CommonStorageKeyValueDB } from './commonStorageKeyValueDB'
import { InMemoryCommonStorage } from './inMemoryCommonStorage'

const storage = new InMemoryCommonStorage()

const db = new CommonStorageKeyValueDB({
  storage,
  bucketName: 'TEST_BUCKET',
})

describe(`runCommonStorageKeyValueDBTest`, () => runCommonKeyValueDBTest(db))
