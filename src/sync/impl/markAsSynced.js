// @flow

import areRecordsEqual from '../../utils/fp/areRecordsEqual'
import { logError, logger } from '../../utils/common'
import type { Database, Model, TableName } from '../..'

import { prepareMarkAsSynced } from './helpers'
import type { SyncLocalChanges, SyncRejectedIds } from '../index'

const recordsToMarkAsSynced = (
  { changes, affectedRecords }: SyncLocalChanges,
  allRejectedIds: SyncRejectedIds,
): Model[] => {
  const syncedRecords = []

  Object.keys(changes).forEach((table) => {
    const { created, updated } = changes[(table: any)]
    const raws = created.concat(updated)
    const rejectedIds = new Set(allRejectedIds[(table: any)])

    raws.forEach((raw) => {
      const { id } = raw
      const record = affectedRecords.find((model) => model.id === id && model.table === table)
      if (!record) {
        logError(
          `[Sync] Looking for record ${table}#${id} to mark it as synced, but I can't find it. Will ignore it (it should get synced next time). This is probably a Watermelon bug — please file an issue!`,
        )
        return
      }
      if (areRecordsEqual(record._raw, raw) && !rejectedIds.has(id)) {
        syncedRecords.push(record)
      }
    })
  })
  return syncedRecords
}

const destroyDeletedRecordsPerCollection = async (
  db: Database,
  { changes }: SyncLocalChanges,
  allRejectedIds: SyncRejectedIds,
): Promise<void> => {
  const tableNames = Object.keys(changes)
  for (let i = 0; i < tableNames.length; i++) {
    const _tableName = tableNames[i]
    const tableName: TableName<any> = (_tableName: any)
    const rejectedIds = new Set(allRejectedIds[tableName])
    const deleted = changes[tableName].deleted.filter((id) => !rejectedIds.has(id))
    if (deleted.length) {
      try {
        await db.adapter.destroyDeletedRecords(tableName, deleted)
      } catch (error) {
        logger.warn(
          `[Sync] destroyDeletedRecords failed for table ${tableName}. Records that were not destroyed will remain in deleted state and be retried on next sync.`,
        )
        throw error
      }
    }
  }
}

export default function markLocalChangesAsSynced(
  db: Database,
  syncedLocalChanges: SyncLocalChanges,
  rejectedIds?: ?SyncRejectedIds,
): Promise<void> {
  return db.write(async () => {
    await db.experimentalBatchNotifications(async () => {
      await destroyDeletedRecordsPerCollection(db, syncedLocalChanges, rejectedIds || {})
      await db.batch(
        recordsToMarkAsSynced(syncedLocalChanges, rejectedIds || {}).map(prepareMarkAsSynced),
      )
    })
  }, 'sync-markLocalChangesAsSynced')
}
