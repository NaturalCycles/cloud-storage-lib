import { InMemoryCommonStorage } from './inMemoryCommonStorage'
import { runCommonStorageTest } from './testing/commonStorageTest'

const storage = new InMemoryCommonStorage()

describe(`runCommonStorageTest`, () => runCommonStorageTest(storage, 'TEST_BUCKET'))
