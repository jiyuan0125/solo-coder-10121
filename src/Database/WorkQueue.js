// @flow
/* eslint-disable no-use-before-define */

import { invariant, logger } from '../utils/common'
import type Model from '../Model'
import type Database from './index'

export interface ReaderInterface {
  /**
   * Calls a Reader so that it runs as part of the current Reader (or Writer) instead of deadlocking.
   *
   * Specifically, the passed block should immediately call a method decorted with `@reader` or a
   * function whose implementation is wrapped in `db.read()` block.
   *
   * See docs for more details.
   *
   * @example
   * ```
   * db.read(async reader => {
   *   // ...
   *   reader.callReader(() => someOtherReader())
   * })
   * ```
   */
  callReader<T>(reader: () => Promise<T>): Promise<T>;
}

export interface WriterInterface extends ReaderInterface {
  /**
   * Calls another Writer so that it runs as part of the current Writer instead of deadlocking.
   *
   * Specifically, the passed block should immediately call a method decorated with `@writer` or
   * a function whose implementation is wrapped in `db.write()` block.
   *
   * See docs for more details.
   *
   * @example
   * ```
   * db.write(async writer => {
   *   // ...
   *   writer.callWriter(() => someOtherWriter())
   * })
   * ```
   */
  callWriter<T>(writer: () => Promise<T>): Promise<T>;

  /** @see {Database#batch} */
  batch(...records: $ReadOnlyArray<Model | Model[] | null | void | false>): Promise<void>;
}

class ReaderInterfaceImpl implements ReaderInterface {
  __workItem: WorkQueueItem<any>
  __workQueue: WorkQueue

  constructor(queue: WorkQueue, item: WorkQueueItem<any>): void {
    this.__workQueue = queue
    this.__workItem = item
  }

  __validateQueue(): void {
    invariant(
      this.__workQueue._queue[0] === this.__workItem,
      'Illegal call on a reader/writer that should no longer be running',
    )
  }

  callReader<T>(reader: () => Promise<T>): Promise<T> {
    this.__validateQueue()
    return this.__workQueue.subAction(reader)
  }
}

class WriterInterfaceImpl extends ReaderInterfaceImpl implements WriterInterface {
  callWriter<T>(writer: () => Promise<T>): Promise<T> {
    this.__validateQueue()
    return this.__workQueue.subAction(writer)
  }

  batch(...records: any): Promise<any> {
    this.__validateQueue()
    return this.__workQueue._db.batch(records)
  }
}

const actionInterface = (queue: WorkQueue, item: WorkQueueItem<any>) =>
  item.isWriter ? new WriterInterfaceImpl(queue, item) : new ReaderInterfaceImpl(queue, item)

type WorkQueueItem<T> = $Exact<{
  work: (ReaderInterface | WriterInterface) => Promise<T>,
  isWriter: boolean,
  resolve: (value: T) => void,
  reject: (reason: any) => void,
  description: ?string,
}>

export default class WorkQueue {
  _db: Database

  _queue: WorkQueueItem<any>[] = []

  _subActionIncoming: boolean = false

  _inSynchronousAction: boolean = false

  _workExecutionDepth: number = 0

  // Set to true only when we're about to execute work function body (and not yet returned)
  // This precisely marks code that is running inside a work function, as opposed to
  // external code that happens to run while a work item is pending in the queue
  _insideWorkExecution: boolean = false

  constructor(db: Database): void {
    this._db = db
  }

  get isWriterRunning(): boolean {
    const [item] = this._queue
    return Boolean(item && item.isWriter)
  }

  enqueue<T>(
    work: ($FlowFixMe<ReaderInterface | WriterInterface>) => Promise<T>,
    description: ?string,
    isWriter: boolean,
  ): Promise<T> {
    // If a subAction was scheduled using subAction(), database.write/read() calls skip the line
    if (this._subActionIncoming) {
      this._subActionIncoming = false
      const currentWork = this._queue[0]
      if (!currentWork.isWriter) {
        invariant(
          !isWriter,
          'Cannot call a writer block from a reader block. ' +
            'Use reader.callReader() to call another reader from within a reader. ' +
            'See docs for more details.',
        )
      }
      // Track execution depth for nested subActions as well
      this._workExecutionDepth += 1
      const wasInsideWorkExecution = this._insideWorkExecution
      this._insideWorkExecution = true
      try {
        const result = work(actionInterface(this, currentWork))
        if (result instanceof Promise) {
          return result.finally(() => {
            this._workExecutionDepth -= 1
            if (this._workExecutionDepth === 0) {
              this._insideWorkExecution = false
            }
          })
        }
        this._workExecutionDepth -= 1
        if (this._workExecutionDepth === 0) {
          this._insideWorkExecution = wasInsideWorkExecution
        }
        return result
      } catch (error) {
        this._workExecutionDepth -= 1
        if (this._workExecutionDepth === 0) {
          this._insideWorkExecution = wasInsideWorkExecution
        }
        throw error
      }
    }

    // Detect potentially illegal nested writer/reader calls that happened after an await boundary
    // _insideWorkExecution means we're currently executing code inside a work() function
    // (synchronous or async phase)
    // !_inSynchronousAction means we've already passed the initial synchronous phase
    // (i.e. there was an await somewhere inside the work function).
    // Together this MIGHT mean: someone called db.write/db.read AFTER awaiting something inside
    // a writer/reader without using callWriter/callReader. This causes deadlocks or crashes.
    // However, this condition can also trigger for LEGAL external calls that happen to run
    // in the same tick while a work item is pending, so we warn instead of throwing.
    // The definitive check is in subAction() which tracks _subActionIncoming synchronously.
    // Synchronous nested calls (without await in between) are always allowed.
    // Also skip check if database is being reset.
    if (
      process.env.NODE_ENV !== 'production' &&
      this._insideWorkExecution &&
      !this._inSynchronousAction &&
      !this._db._isBeingReset
    ) {
      const currentWork = this._queue[0]
      const currentKind = currentWork && currentWork.isWriter ? 'writer' : 'reader'
      const enqueuedKind = isWriter ? 'writer' : 'reader'
      const description = currentWork ? currentWork.description || 'unnamed' : 'unnamed'
      logger.warn(
        `Potentially illegal nested ${enqueuedKind} call detected! ` +
          `You are trying to call a ${enqueuedKind} while a ${currentKind} ` +
          `(${description}) is already running. ` +
          `If this call happened AFTER awaiting an async operation inside the running ${currentKind}, ` +
          `this WILL cause deadlocks or crashes. ` +
          `Use writer.callWriter() or reader.callReader() to safely nest readers/writers. ` +
          `If this call happened from OUTSIDE any reader/writer (external code), ` +
          `this is just a warning - the call will be queued normally. ` +
          `See docs for more details.`,
      )
      logger.log(`Enqueued ${enqueuedKind}:`, work)
      if (currentWork) {
        logger.log(`Running ${currentKind}:`, currentWork.work)
      }
    }

    return new Promise((resolve, reject) => {
      const workItem: WorkQueueItem<T> = { work, isWriter, resolve, reject, description }

      if (process.env.NODE_ENV !== 'production' && this._queue.length) {
        setTimeout(() => {
          const queue = this._queue
          const current = queue[0]
          if (current === workItem || !queue.includes(workItem)) {
            return
          }

          const enqueuedKind = isWriter ? 'writer' : 'reader'
          const currentKind = current.isWriter ? 'writer' : 'reader'
          logger.warn(
            `The ${enqueuedKind} you're trying to run (${
              description || 'unnamed'
            }) can't be performed yet, because there are ${
              queue.length
            } other readers/writers in the queue.\n\nCurrent ${currentKind}: ${
              current.description || 'unnamed'
            }.\n\nIf everything is working fine, you can safely ignore this message (queueing is working as expected). But if your readers/writers are not running, it's because the current ${currentKind} is stuck.\nRemember that if you're calling a reader/writer from another reader/writer, you must use callReader()/callWriter(). See docs for more details.`,
          )
          logger.log(`Enqueued ${enqueuedKind}:`, work)
          logger.log(`Running ${currentKind}:`, current.work)
        }, 1500)
      }

      this._queue.push(workItem)

      if (this._queue.length === 1) {
        this._executeNext()
      }
    })
  }

  subAction<T>(work: () => Promise<T>): Promise<T> {
    try {
      this._subActionIncoming = true
      const promise = work()
      invariant(
        !this._subActionIncoming,
        'callReader/callWriter call must call a reader/writer synchronously. ' +
          'You cannot await an async operation before calling the nested reader/writer. ' +
          'If you need to do async work before the nested call, perform it before calling callReader/callWriter, ' +
          'or restructure your code to call the nested reader/writer synchronously. ' +
          'See docs for more details.',
      )
      return promise
    } catch (error) {
      this._subActionIncoming = false
      return Promise.reject(error)
    }
  }

  async _executeNext(): Promise<void> {
    const workItem = this._queue[0]
    const { work, resolve, reject, isWriter } = workItem

    let workPromise
    let result
    let caughtError
    try {
      // Mark that we're in the synchronous execution phase of a work item
      // This is used to detect illegal nested writer/reader calls
      // Only set if not already set (i.e. we're the top-level work, not a subAction)
      const wasInSynchronousAction = this._inSynchronousAction
      if (!wasInSynchronousAction) {
        this._inSynchronousAction = true
      }
      // Increment work execution depth (including for subActions)
      // This tracks whether we're inside any work() function at all
      this._workExecutionDepth += 1
      // Mark that we're about to execute inside a work function body
      // This is used together with _inSynchronousAction to detect illegal nested calls
      this._insideWorkExecution = true
      if (process.env.NODE_ENV !== 'production' && isWriter && !wasInSynchronousAction) {
        this._db._preparedRecordsInWriter.clear()
      }
      workPromise = work(actionInterface(this, workItem))
      if (!wasInSynchronousAction) {
        this._inSynchronousAction = false
      }

      if (process.env.NODE_ENV !== 'production') {
        invariant(
          workPromise instanceof Promise,
          `The function passed to database.${
            isWriter ? 'write' : 'read'
          }() or a method marked as @${
            isWriter ? 'writer' : 'reader'
          } must be asynchronous (marked as 'async' or always returning a promise) (in: ${
            workItem.description || 'unnamed'
          })`,
        )
      }

      result = await workPromise

      if (process.env.NODE_ENV !== 'production' && isWriter && !wasInSynchronousAction) {
        const uncommitted = this._db._preparedRecordsInWriter
        if (uncommitted.size > 0) {
          const recordList = Array.from(uncommitted)
          const debugNames = recordList
            .slice(0, 5)
            .map((r: Model) => r.__debugName)
            .join(', ')
          const more = uncommitted.size > 5 ? ` (and ${uncommitted.size - 5} more)` : ''
          logger.warn(
            `Prepared changes were not sent to batch() in writer (${workItem.description ||
              'unnamed'}): ${debugNames}${more}. ` +
              `Use database.batch() to commit prepared changes before the writer returns. See docs for more details.`,
          )
        }
      }
    } catch (error) {
      // Ensure the flag is reset even if work throws synchronously or asynchronously
      this._inSynchronousAction = false
      caughtError = error
    } finally {
      this._workExecutionDepth -= 1
      // Reset _insideWorkExecution only if this was the outermost work execution
      // (i.e. don't reset it prematurely when inside a nested subAction)
      if (this._workExecutionDepth === 0) {
        this._insideWorkExecution = false
      }
    }

    this._queue.shift()

    if (caughtError) {
      reject(caughtError)
    } else {
      resolve(result)
    }

    if (this._queue.length) {
      setTimeout(() => this._executeNext(), 0)
    }
  }

  _abortPendingWork(): void {
    invariant(this._queue.length >= 1, '_abortPendingWork can only be called from a reader/writer')
    const workToAbort = this._queue.splice(1) // leave only the caller on the queue
    workToAbort.forEach(({ reject }) => {
      reject(new Error('Reader/writer has been aborted because the database was reset'))
    })
  }
}
