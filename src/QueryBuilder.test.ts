import { erc20Abi } from 'abitype/abis'
import { createPublicClient, http } from 'viem'
import { base } from 'viem/chains'
import { describe, expect, test } from 'vitest'
import * as IS from './IndexSupply.js'
import * as QueryBuilder from './QueryBuilder.js'

const client = createPublicClient({
  chain: base,
  transport: http(),
})

const is = IS.create({
  apiKey: process.env.VITE_API_KEY,
})

describe('from', () => {
  test('default', async () => {
    const queryBuilder = QueryBuilder.from(is)
    expect(queryBuilder).toMatchInlineSnapshot(`
      {
        "atCursor": [Function],
        "selectFrom": [Function],
        "withAbi": [Function],
        "withSignatures": [Function],
      }
    `)
  })

  test('behavior: withSignatures creates event table', async () => {
    const qb = QueryBuilder.from(is).withSignatures([
      'event Transfer(address indexed from, address indexed to, uint256 value)',
    ])

    const result = await qb
      .selectFrom('transfer')
      .select(['from', 'to', 'value'])
      .where('chain', '=', 8453)
      .limit(10)
      .execute()

    expect(Array.isArray(result)).toBe(true)
    expect(result.length).toBeLessThanOrEqual(10)

    for (const row of result) {
      expect(row).toHaveProperty('from')
      expect(row).toHaveProperty('to')
      expect(row).toHaveProperty('value')
    }
  })

  test('behavior: withAbi creates multiple event tables', async () => {
    const qb = QueryBuilder.from(is).withAbi(erc20Abi)

    // Query the Transfer event table
    const transfers = await qb
      .selectFrom('transfer')
      .select(['from', 'to', 'value'])
      .where('chain', '=', 8453)
      .limit(5)
      .execute()

    expect(Array.isArray(transfers)).toBe(true)

    // Query the Approval event table
    const approvals = await qb
      .selectFrom('approval')
      .select(['owner', 'spender', 'value'])
      .where('chain', '=', 8453)
      .limit(5)
      .execute()

    expect(Array.isArray(approvals)).toBe(true)
  })

  test('behavior: withSignatures with multiple events', async () => {
    const qb = QueryBuilder.from(is).withSignatures([
      'event Transfer(address indexed from, address indexed to, uint256 value)',
      'event Approval(address indexed owner, address indexed spender, uint256 value)',
    ])

    const transfers = await qb
      .selectFrom('transfer')
      .select(['from', 'to', 'value'])
      .where('chain', '=', 8453)
      .limit(3)
      .execute()

    expect(Array.isArray(transfers)).toBe(true)

    const approvals = await qb
      .selectFrom('approval')
      .select(['owner', 'spender', 'value'])
      .where('chain', '=', 8453)
      .limit(3)
      .execute()

    expect(Array.isArray(approvals)).toBe(true)
  })

  test('behavior: chain withAbi calls', async () => {
    const qb = QueryBuilder.from(is)
      .withAbi(erc20Abi)
      .withSignatures(['event Deposit(address indexed user, uint256 amount)'])

    // Can query both Transfer and Deposit
    const transfers = await qb
      .selectFrom('transfer')
      .select(['from', 'to'])
      .where('chain', '=', 8453)
      .limit(2)
      .execute()

    expect(Array.isArray(transfers)).toBe(true)

    const deposits = await qb
      .selectFrom('deposit')
      .select(['user', 'amount'])
      .where('chain', '=', 8453)
      .limit(2)
      .execute()

    expect(Array.isArray(deposits)).toBe(true)
  })

  test('behavior: withSignatures with complex event types', async () => {
    const qb = QueryBuilder.from(is).withSignatures([
      'event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256[] ids, uint256[] values)',
    ])

    const result = await qb
      .selectFrom('transferbatch')
      .select(['operator', 'from', 'to', 'ids', 'values'])
      .where('chain', '=', 8453)
      .limit(5)
      .execute()

    expect(Array.isArray(result)).toBe(true)
  })

  test('behavior: fetch transactions for address', async () => {
    const address = '0x0000000000000000000000000000000000000000'
    const chainId = 8453
    const limit = 10
    const offset = 0

    const qb = QueryBuilder.from(is)

    // Fetch transactions where address is sender or receiver
    const result = await qb
      .selectFrom('txs')
      .select([
        'hash',
        'block_num',
        'from',
        'to',
        'value',
        'input',
        'nonce',
        'gas',
        'gas_price',
        'type',
      ])
      .where('chain', '=', chainId)
      .where((eb) => eb.or([eb('from', '=', address), eb('to', '=', address)]))
      .orderBy('block_num', 'desc')
      .limit(limit)
      .offset(offset)
      .execute()

    expect(Array.isArray(result)).toBe(true)
    expect(result.length).toBeLessThanOrEqual(limit)
    expect((result as unknown as { cursor: string }).cursor).toBeDefined()

    // Verify each transaction has the expected fields
    for (const tx of result) {
      expect(tx).toHaveProperty('hash')
      expect(tx).toHaveProperty('block_num')
      expect(tx).toHaveProperty('from')
      expect(tx).toHaveProperty('to')
      expect(tx).toHaveProperty('value')

      // Verify address filter worked
      const txTyped = tx as { from: string; to: string }
      expect(txTyped.from === address || txTyped.to === address).toBe(true)
    }
  })

  test('behavior: count transactions for address', async () => {
    const address = '0x0000000000000000000000000000000000000000'
    const chainId = 8453
    const blockNumber = await client.getBlockNumber()

    const qb = QueryBuilder.from(is)

    const result = await qb
      .atCursor({ chainId, blockNumber })
      .selectFrom('txs')
      .select((eb) => [eb.fn.count('txs.hash').as('total')])
      .where('txs.to', '=', address)
      .where('txs.chain', '=', chainId)
      .execute()

    expect(result.length).toBe(1)
    expect(result[0]).toHaveProperty('total')
    const firstResult = result[0] as { total: unknown }
    expect(typeof firstResult.total === 'number' || typeof firstResult.total === 'string').toBe(
      true,
    )
  })

  test('behavior: fetch sent transactions only', async () => {
    const address = '0x5de176348c089b9709baf01c5d0edbbb82f2a8a6'
    const chainId = 8453
    const limit = 5

    const qb = QueryBuilder.from(is)

    const result = await qb
      .selectFrom('txs')
      .select(['hash', 'from', 'to', 'block_num'])
      .where('chain', '=', chainId)
      .where('from', '=', address)
      .orderBy('block_num', 'desc')
      .limit(limit)
      .execute()

    expect(Array.isArray(result)).toBe(true)

    // Verify all transactions are sent from the address
    for (const tx of result) {
      const txTyped = tx as { from: string }
      expect(txTyped.from).toBe(address)
    }
  })

  test('behavior: fetch received transactions only', async () => {
    const address = '0x4200000000000000000000000000000000000006'
    const chainId = 8453
    const limit = 5

    const qb = QueryBuilder.from(is)

    const result = await qb
      .selectFrom('txs')
      .select(['hash', 'from', 'to', 'block_num'])
      .where('chain', '=', chainId)
      .where('to', '=', address)
      .orderBy('block_num', 'desc')
      .limit(limit)
      .execute()

    expect(Array.isArray(result)).toBe(true)

    // Verify all transactions are sent to the address
    for (const tx of result) {
      expect(tx.to).toBe(address)
    }
  })

  test('behavior: pagination with offset', async () => {
    const chainId = 8453
    const limit = 3
    const qb = QueryBuilder.from(is)

    // Fetch first page
    const page1 = await qb
      .selectFrom('txs')
      .select(['hash', 'block_num'])
      .where('chain', '=', chainId)
      .orderBy('block_num', 'desc')
      .limit(limit)
      .offset(0)
      .execute()

    // Fetch second page
    const page2 = await qb
      .selectFrom('txs')
      .select(['hash', 'block_num'])
      .where('chain', '=', chainId)
      .orderBy('block_num', 'desc')
      .limit(limit)
      .offset(limit)
      .execute()

    expect(page1.length).toBeLessThanOrEqual(limit)
    expect(page2.length).toBeLessThanOrEqual(limit)

    // Verify pages don't overlap
    const page1Hashes = new Set(page1.map((tx) => tx.hash))
    const page2Hashes = new Set(page2.map((tx) => tx.hash))

    for (const hash of page2Hashes) {
      expect(page1Hashes.has(hash)).toBe(false)
    }
  })

  test('behavior: using cursor for pagination', async () => {
    const chainId = 8453
    const limit = 5
    const qb = QueryBuilder.from(is)

    // Fetch first page
    const page1 = await qb
      .selectFrom('txs')
      .select(['hash', 'block_num'])
      .where('chain', '=', chainId)
      .orderBy('block_num', 'desc')
      .limit(limit)
      .execute()

    const cursor = (page1 as unknown as { cursor: string }).cursor
    expect(cursor).toBeDefined()
    expect(typeof cursor).toBe('string')

    // Fetch next page using cursor
    const page2 = await qb
      .atCursor(cursor)
      .selectFrom('txs')
      .select(['hash', 'block_num'])
      .where('chain', '=', chainId)
      .orderBy('block_num', 'desc')
      .limit(limit)
      .execute()

    expect(page2.length).toBeLessThanOrEqual(limit)
  })

  test('behavior: streaming query with live endpoint', async () => {
    const qb = QueryBuilder.from(is).withSignatures([
      'event Transfer(address indexed from, address indexed to, uint256 value)',
    ])

    const stream = qb
      .selectFrom('transfer')
      .select(['from', 'to', 'value'])
      .where('chain', '=', 8453)
      .limit(5)
      .stream()

    const results: unknown[] = []
    let count = 0
    const maxResults = 3

    for await (const result of stream) {
      results.push(result)
      count++
      if (count >= maxResults) break
    }

    expect(results.length).toBeGreaterThan(0)
    expect(results.length).toBeLessThanOrEqual(maxResults)
  })
})
