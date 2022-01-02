import { requireEnvKeys } from '@naturalcycles/nodejs-lib'
import * as admin from 'firebase-admin'
import { CloudStorage } from '../cloudStorage'
import { runCommonStorageTest } from '../testing/commonStorageTest'

const { FIREBASE_SERVICE_ACCOUNT, FIREBASE_BUCKET } = requireEnvKeys(
  'FIREBASE_SERVICE_ACCOUNT',
  'FIREBASE_BUCKET',
)

const credential = admin.credential.cert(JSON.parse(FIREBASE_SERVICE_ACCOUNT))

const app = admin.initializeApp({
  credential,
  // storageBucket: FIREBASE_BUCKET,
})

const storage = new CloudStorage(app.storage() as any)

describe(`runCommonStorageTest`, () => runCommonStorageTest(storage, FIREBASE_BUCKET))

test('listFiles', async () => {
  const files = await storage.getFileNames(FIREBASE_BUCKET)
  console.log(files)
})
