// @flow
import { mockDatabase } from './testModels'
import { logger } from '../utils/common'

describe('prepared records await boundary detection', () => {
  it('sync prepare + sync batch → no warn', async () => {
    const { db, tasks } = mockDatabase()

    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {})

    await db.write(async () => {
      const task = await tasks.create((t) => {
        t.name = 'test'
      })
      const prepared = task.prepareUpdate((t) => {
        t.name = 'updated'
      })
      await db.batch(prepared)
    }, 'test-sync-batch')

    const hasAwaitBoundaryWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('batched after an await boundary'),
    )
    const hasUncommittedWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('not sent to batch()'),
    )

    warnSpy.mockRestore()

    expect(hasAwaitBoundaryWarn).toBe(false)
    expect(hasUncommittedWarn).toBe(false)
  })

  it('prepare + await + batch → must warn about await boundary', async () => {
    const { db, tasks } = mockDatabase()

    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {})

    await db.write(async () => {
      const task = await tasks.create((t) => {
        t.name = 'test2'
      })
      const prepared = task.prepareUpdate((t) => {
        t.name = 'updated2'
      })
      await Promise.resolve()
      await db.batch(prepared)
    }, 'test-await-then-batch')

    const hasAwaitBoundaryWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('batched after an await boundary'),
    )
    const hasRecordName = warnSpy.mock.calls.some(
      (call) =>
        String(call[0]).includes('mock_tasks') ||
        String(call[0]).includes('MockTask') ||
        String(call[0]).includes('Task'),
    )

    warnSpy.mockRestore()

    expect(hasAwaitBoundaryWarn).toBe(true)
    expect(hasRecordName).toBe(true)
  })

  it('prepare + await + NO batch → must warn (uncommitted)', async () => {
    const { db, tasks } = mockDatabase()

    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {})

    try {
      await db.write(async () => {
        const task = await tasks.create((t) => {
          t.name = 'test3'
        })
        const prepared = task.prepareUpdate((t) => {
          t.name = 'updated3'
        })
        await Promise.resolve()
        // intentionally no batch
        void prepared
      }, 'test-no-batch')
    } catch (e) {
      // may throw due to prepared state validation when writer exits, that's ok
    }

    const hasUncommittedWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('not sent to batch()'),
    )

    warnSpy.mockRestore()

    expect(hasUncommittedWarn).toBe(true)
  })

  it('prepareMarkAsDeleted + await + batch → must warn about await boundary', async () => {
    const { db, tasks } = mockDatabase()

    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {})

    await db.write(async () => {
      const task = await tasks.create((t) => {
        t.name = 'test4'
      })
      const prepared = task.prepareMarkAsDeleted()
      await Promise.resolve()
      await db.batch(prepared)
    }, 'test-delete-await-batch')

    const hasAwaitBoundaryWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('batched after an await boundary'),
    )

    warnSpy.mockRestore()

    expect(hasAwaitBoundaryWarn).toBe(true)
  })

  it('fetchDescendants-like pattern: await then prepare then sync batch → no await boundary warn (prepare is after await)', async () => {
    // If prepare happens AFTER the await (not in sync phase), it shouldn't trigger
    // the "batched after await boundary" warning because the prepare itself was after await
    const { db, tasks } = mockDatabase()

    const warnSpy = jest.spyOn(logger, 'warn').mockImplementation(() => {})

    await db.write(async () => {
      await Promise.resolve() // simulate fetchDescendants async call
      const task = await tasks.create((t) => {
        t.name = 'test5'
      })
      const prepared = task.prepareUpdate((t) => {
        t.name = 'updated5'
      })
      await db.batch(prepared) // batch right after prepare (same tick)
    }, 'test-await-then-prepare-then-batch')

    // prepare happened after await, so it's not in sync phase → no await boundary warning
    const hasAwaitBoundaryWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('batched after an await boundary'),
    )
    // But it was batched successfully, so no uncommitted warning either
    const hasUncommittedWarn = warnSpy.mock.calls.some((call) =>
      String(call[0]).includes('not sent to batch()'),
    )

    warnSpy.mockRestore()

    expect(hasAwaitBoundaryWarn).toBe(false)
    expect(hasUncommittedWarn).toBe(false)
  })
})
