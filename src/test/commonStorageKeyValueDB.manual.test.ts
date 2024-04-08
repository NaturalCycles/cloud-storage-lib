import { runCommonKeyValueDBTest } from '@naturalcycles/db-lib/dist/testing'
import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import { CloudStorage } from '../cloudStorage'
import { CommonStorageKeyValueDB } from '../commonStorageKeyValueDB'
import { GCPServiceAccount } from '../model'

const { bucketName, GCP_SERVICE_ACCOUNT: serviceAccountStr } = requireEnvKeys(
  'bucketName',
  'GCP_SERVICE_ACCOUNT',
)
const serviceAccount: GCPServiceAccount = JSON.parse(serviceAccountStr)

const storage = CloudStorage.createFromGCPServiceAccount(serviceAccount)

const db = new CommonStorageKeyValueDB({
  storage,
  bucketName,
})

describe(`runCommonStorageTest`, () => runCommonKeyValueDBTest(db))
