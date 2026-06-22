// @flow

import { type Observable, startWith, merge as merge$ } from '../utils/rx'
import { type Unsubscribe } from '../utils/subscriptions'
import { invariant, logger } from '../utils/common'
import {
  noop,
  fromArrayOrSpread,
  // eslint-disable-next-line no-unused-vars
  type ArrayOrSpreadFn,
} from '../utils/fp'

import type { DatabaseAdapter, BatchOperation } from '../adapters/type'
import DatabaseAdapterCompat from '../adapters/compat'
import type Model from '../Model'
import type Collection, { CollectionChangeSet } from '../Collection'
import type { TableName, AppSchema } from '../Schema'

import CollectionMap from './CollectionMap'
import type LocalStorage from './LocalStorage'
import WorkQueue, { type ReaderInterface, type WriterInterface } from './WorkQueue'

type DatabaseProps = $Exact<{
  adapter: DatabaseAdapter,
  modelClasses: Array<Class<Model>>,
}>

let experimentalAllowsFatalError = false

export function setExperimentalAllowsFatalError(): void {
  experimentalAllowsFatalError = true
}

export default class Database {
  /**
   * Database's adapter - the low-level connection with the underlying database (e.g. SQLite)
   *
   * Unless you understand WatermelonDB's internals, you SHOULD NOT use adapter directly.
   * Running queries, or updating/deleting records on the adapter will corrupt the in-memory cache
   * if special care is not taken
   */
  adapter: DatabaseAdapterCompat

  schema: AppSchema

  collections: CollectionMap

  _workQueue: WorkQueue = new WorkQueue(this)

  // (experimental) if true, Database is in a broken state and should not be used anymore
  _isBroken: boolean = false

  _localStorage: LocalStorage

  constructor(options: DatabaseProps): void {
    const { adapter, modelClasses } = options
    if (process.env.NODE_ENV !== 'production') {
      invariant(adapter, `Missing adapter parameter for new Database()`)
      invariant(
        modelClasses && Array.isArray(modelClasses),
        `Missing modelClasses parameter for new Database()`,
      )
    }
    this.adapter = new DatabaseAdapterCompat(adapter)
    this.schema = adapter.schema
    this.collections = new CollectionMap(this, modelClasses)
  }

  /**
   * Returns a `Collection` for a given table name
   */
  get<T: Model>(tableName: TableName<T>): Collection<T> {
    return this.collections.get(tableName)
  }

  /**
   * Returns a `LocalStorage` (WatermelonDB-based localStorage/AsyncStorage alternative)
   */
  get localStorage(): LocalStorage {
    if (!this._localStorage) {
      const LocalStorageClass = require('./LocalStorage').default
      this._localStorage = new LocalStorageClass(this)
    }
    return this._localStorage
  }

  /*:: batch: ArrayOrSpreadFn<?Model | false, Promise<void>>  */
  /**
   * Executes multiple prepared operations
   *
   * Pass a list (or array) of operations like so:
   * - `collection.prepareCreate(...)`
   * - `record.prepareUpdate(...)`
   * - `record.prepareMarkAsDeleted()` (or `record.prepareDestroyPermanently()`)
   *
   * Note that falsy values (null, undefined, false) passed to batch are simply ignored
   * so you can use patterns like `.batch(condition && record.prepareUpdate(...))` for convenience.
   *
   * Note: This method must be called within a Writer {@link Database#write}.
   */
  // $FlowFixMe
  async batch(...records: Array<?Model | false>): Promise<void> {
    const actualRecords: Array<?Model> = fromArrayOrSpread(records, 'Database.batch', 'Model')

    this._ensureInWriter(`Database.batch()`)

    // performance critical - using mutations
    const batchOperations: BatchOperation[] = []
    const changeNotifications: { [TableName<any>]: CollectionChangeSet<Model> } = {}
    const recordsToProcess: Model[] = []
    actualRecords.forEach((record) => {
      if (!record) {
        return
      }

      const preparedState = record._preparedState
      if (!preparedState) {
        invariant(record._raw._status !== 'disposable', `Cannot batch a disposable record`)
        throw new Error(`Cannot batch a record that doesn't have a prepared create/update/delete`)
      }

      recordsToProcess.push(record)

      const raw = record._raw
      const { id } = raw // faster than Model.id
      const { table } = record.constructor // faster than Model.table

      let changeType

      if (preparedState === 'update') {
        batchOperations.push(['update', table, raw])
        changeType = 'updated'
      } else if (preparedState === 'create') {
        batchOperations.push(['create', table, raw])
        changeType = 'created'
      } else if (preparedState === 'markAsDeleted') {
        batchOperations.push(['markAsDeleted', table, id])
        changeType = 'destroyed'
      } else if (preparedState === 'destroyPermanently') {
        batchOperations.push(['destroyPermanently', table, id])
        changeType = 'destroyed'
      } else {
        invariant(false, 'bad preparedState')
      }

      if (!changeNotifications[table]) {
        changeNotifications[table] = []
      }
      changeNotifications[table].push({ record, type: changeType })
    })

    // Detect records that were prepared in a different synchronous segment (crossed await boundary)
    // Must do this in the sync phase before awaiting adapter.batch()
    if (process.env.NODE_ENV !== 'production') {
      const currentGen = this._workQueue._syncGeneration
      recordsToProcess.forEach((record) => {
        if (
          record._preparedSyncGeneration !== null &&
          record._preparedSyncGeneration !== currentGen
        ) {
          this._preparedAcrossAwaitBoundary.add(record)
        }
      })
    }

    // NOTE: We clear _preparedState BEFORE awaiting adapter.batch(). This ensures that the
    // record appears "clean" (no pending changes) synchronously after batch() is called,
    // matching the original API contract. If adapter.batch() fails, _revertPreparedChanges()
    // will restore both the raw data AND the _preparedState so the record can be retried.
    recordsToProcess.forEach((record) => {
      record._preparedStateBeforeBatch = record._preparedState
      record._preparedState = null
    })

    try {
      await this.adapter.batch(batchOperations)
    } catch (error) {
      recordsToProcess.forEach((record) => {
        record._revertPreparedChanges()
      })
      throw error
    }

    // Debug info
    if (this.experimentalIsVerbose) {
      const debugInfo = batchOperations
        .map(([type, table, rawOrId]) => {
          switch (type) {
            case 'create':
            case 'update':
              return `${type} ${table}#${(rawOrId: any).id}`
            case 'markAsDeleted':
            case 'destroyPermanently':
              return `${type} ${table}#${(rawOrId: any)}`
            default:
              return `${type}???`
          }
        })
        .join(', ')
      logger.debug(`batch: ${debugInfo}`)
    }

    // NOTE: We must make two passes to ensure all changes to caches are applied before subscribers are called
    const changes: [TableName<any>, CollectionChangeSet<any>][] = (Object.entries(
      changeNotifications,
    ): any)

    changes.forEach(([table, changeSet]) => {
      this.collections.get(table)._applyChangesToCache(changeSet)
    })

    this._notify(changes)

    return undefined // shuts up flow
  }

  _notificationBatchStack: [TableName<any>, CollectionChangeSet<any>][][][] = []

  _preparedRecordsInWriter: Set<Model> = new Set()
  _preparedAcrossAwaitBoundary: Set<Model> = new Set()

  _notify(changes: [TableName<any>, CollectionChangeSet<any>][]): void {
    if (this._notificationBatchStack.length > 0) {
      this._notificationBatchStack[this._notificationBatchStack.length - 1].push(changes)
      return
    }

    const affectedTables = new Set(changes.map(([table]) => table))

    const databaseChangeNotifySubscribers = ([tables, subscriber]: [
      Array<TableName<any>>,
      () => void,
      any,
    ]): void => {
      if (tables.some((table) => affectedTables.has(table))) {
        subscriber()
      }
    }
    this._subscribers.forEach(databaseChangeNotifySubscribers)

    changes.forEach(([table, changeSet]) => {
      this.collections.get(table)._notify(changeSet)
    })
  }

  async experimentalBatchNotifications<T>(work: () => Promise<T>): Promise<T> {
    // TODO: Document & add tests if this proves useful
    this._notificationBatchStack.push([])
    try {
      const result = await work()
      this._commitNotificationBatch()
      return result
    } catch (error) {
      this._discardNotificationBatch()
      throw error
    }
  }

  _commitNotificationBatch(): void {
    const currentBatch = this._notificationBatchStack.pop()
    if (!currentBatch) {
      return
    }

    if (this._notificationBatchStack.length === 0) {
      currentBatch.forEach((changes) => {
        this._notify(changes)
      })
    } else {
      const parentBatch = this._notificationBatchStack[this._notificationBatchStack.length - 1]
      ;(parentBatch: any).push(...currentBatch)
    }
  }

  _discardNotificationBatch(): void {
    this._notificationBatchStack.pop()
  }

  /**
   * Schedules a Writer
   *
   * Writer is a block of code, inside of which you can modify the database
   * (call `Collection.create`, `Model.update`, `Database.batch` and so on).
   *
   * In a Writer, you're guaranteed that no other Writer is simultaneously executing. Therefore, you
   * can rely on the results of queries and other asynchronous operations - they won't change for
   * the duration of this Writer (except if changed by it).
   *
   * To call another Writer (or Reader) from this one without deadlocking, use `callWriter`
   * (or `callReader`).
   *
   * See docs for more details and a practical guide.
   *
   * @param work - Block of code to execute
   * @param [description] - Debug description of this Writer
   */
  write<T>(work: (WriterInterface) => Promise<T>, description?: string): Promise<T> {
    return this._workQueue.enqueue(work, description, true)
  }

  /**
   * Schedules a Reader
   *
   * In a Reader, you're guaranteed that no Writer is running at the same time. Therefore, you can
   * run many queries or other asynchronous operations, and you can rely on their results - they
   * won't change for the duration of this Reader. However, other Readers might run concurrently.
   *
   * To call another Reader from this one, use `callReader`
   *
   * See docs for more details and a practical guide.
   *
   * @param work - Block of code to execute
   * @param [description] - Debug description of this Reader
   */
  read<T>(work: (ReaderInterface) => Promise<T>, description?: string): Promise<T> {
    return this._workQueue.enqueue(work, description, false)
  }

  /**
   * Returns an `Observable` that emits a signal (`null`) immediately, and on every change in
   * any of the passed tables.
   *
   * A set of changes made is passed with the signal, with an array of changes per-table
   * (Currently, if changes are made to multiple different tables, multiple signals will be emitted,
   * even if they're made with a batch. However, this behavior might change. Use Rx to debounce,
   * throttle, merge as appropriate for your use case.)
   *
   * Warning: You can easily introduce performance bugs in your application by using this method
   * inappropriately.
   */
  withChangesForTables(tables: TableName<any>[]): Observable<CollectionChangeSet<any> | null> {
    const changesSignals = tables.map((table) => this.collections.get(table).changes)

    return merge$(...changesSignals).pipe(startWith(null))
  }

  _subscribers: [TableName<any>[], () => void, any][] = []

  /**
   * Notifies `subscriber` on change in any of the passed tables.
   *
   * A single notification will be sent per `database.batch()` call.
   * (Currently, no details about the changes made are provided, only a signal, but this behavior
   * might change. Currently, subscribers are called before `withChangesForTables`).
   *
   * Warning: You can easily introduce performance bugs in your application by using this method
   * inappropriately.
   */
  experimentalSubscribe(
    tables: TableName<any>[],
    subscriber: () => void,
    debugInfo?: any,
  ): Unsubscribe {
    if (!tables.length) {
      return noop
    }

    const entry = [tables, subscriber, debugInfo]
    this._subscribers.push(entry)

    return () => {
      const idx = this._subscribers.indexOf(entry)
      idx !== -1 && this._subscribers.splice(idx, 1)
    }
  }

  _resetCount: number = 0

  _isBeingReset: boolean = false

  /**
   * Resets the database
   *
   * This permanently deletes the database (all records, metadata, and `LocalStorage`) and sets
   * up an empty database.
   *
   * Special care must be taken to safely reset the database. Ideally, you should reset your app
   * to an empty / "logging out" state while doing this. Specifically:
   *
   * - You MUST NOT hold onto Watermelon records other than this `Database`. Do not keep references
   *   to records, collections, or any other objects from before database reset
   * - You MUST NOT observe any Watermelon state. All Database, Collection, Query, and Model
   *   observers/subscribers should be disposed of before resetting
   * - You SHOULD NOT have any pending (queued) Readers or Writers. Pending work will be aborted
   *   (rejected with an error)
   */
  async unsafeResetDatabase(): Promise<void> {
    this._ensureInWriter(`Database.unsafeResetDatabase()`)
    try {
      this._isBeingReset = true
      this._workQueue._abortPendingWork()

      this._notificationBatchStack = []
      this._preparedRecordsInWriter = new Set()
      this._preparedAcrossAwaitBoundary = new Set()
      this._workQueue._subActionIncoming = false
      this._workQueue._syncGeneration = 0
      this._workQueue._syncGenBumpScheduled = false
      this._workQueue._inSynchronousAction = false
      this._workQueue._insideWorkExecution = false
      this._workQueue._workExecutionDepth = 0

      const { adapter } = this
      const ErrorAdapter = require('../adapters/error').default
      this.adapter = (new ErrorAdapter(): any)

      if (this._subscribers.length) {
        // eslint-disable-next-line no-console
        console.log(
          `Application error! Unexpected ${this._subscribers.length} Database subscribers were detected during database.unsafeResetDatabase() call. App should not hold onto subscriptions or Watermelon objects while resetting database.`,
        )
        // eslint-disable-next-line no-console
        console.log(this._subscribers)
        this._subscribers = []
      }

      await adapter.unsafeResetDatabase()

      Object.values(this.collections.map).forEach((collection) => {
        // $FlowFixMe
        collection._cache.unsafeClear()
      })

      this._resetCount += 1
      this.adapter = adapter
    } finally {
      this._isBeingReset = false
    }
  }

  // (experimental) if true, Models will print to console diagnostic information on every
  // prepareCreate/Update/Delete call, as well as on commit (Database.batch() call). Note that this
  // has a significant performance impact so should only be enabled when debugging.
  experimentalIsVerbose: boolean = false

  _ensureInWriter(debugName: string): void {
    invariant(
      this._workQueue.isWriterRunning,
      `${debugName} can only be called from inside of a Writer. See docs for more details.`,
    )
  }

  // (experimental) puts Database in a broken state
  // TODO: Not used anywhere yet
  _fatalError(error: Error): void {
    if (!experimentalAllowsFatalError) {
      logger.warn(
        'Database is now broken, but experimentalAllowsFatalError has not been enabled to do anything about it...',
      )
      return
    }

    this._isBroken = true
    logger.error('Database is broken. App must be reloaded before continuing.')

    // TODO: Passing this to an adapter feels wrong, but it's tricky.
    // $FlowFixMe
    if (this.adapter.underlyingAdapter._fatalError) {
      // $FlowFixMe
      this.adapter.underlyingAdapter._fatalError(error)
    }
  }
}
