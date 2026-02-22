import { describe, expect, expectTypeOf, test } from 'vitest'
import * as Result from './result.js'

describe('ToRows', () => {
  test('default', () => {
    type Rows = Result.ToRows<
      `select "from", "to", tokens from transfer where block_num > 100 limit 3`,
      ['event Transfer(address indexed from, address indexed to, uint256 tokens)']
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      from: `0x${string}`
      to: `0x${string}`
      tokens: bigint
    }>()
  })

  test('behavior: handles type casts, quoted identifiers, and as aliases', () => {
    type Rows = Result.ToRows<`
      select
        block_timestamp::date,
        hash,
        "to",
        substring(input, 1, 4) as "4b"
      from txs
      where "from" = 0xd8da6bf26964af9d7eed9e03e53415d37aa96045
    `>

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      block_timestamp: number
      hash: `0x${string}`
      to: `0x${string}`
      '4b': string
    }>()
  })

  test('behavior: preserves case in aliases', () => {
    type Rows = Result.ToRows<
      `
      select
        commentIdentifier->>'commenter' as commenter,
        commentIdentifier->>'contractAddress' as contractAddress,
        commentIdentifier->>'tokenId' as tokenId,
        sparker, timestamp, referrer
      from sparkedcomment
      limit 100
    `,
      [
        'event SparkedComment(address indexed commentIdentifier, address indexed sparker, uint256 timestamp, address indexed referrer)',
      ]
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      commenter: string
      contractAddress: string
      tokenId: string
      sparker: `0x${string}`
      timestamp: bigint
      referrer: `0x${string}`
    }>()
  })

  test('behavior: handles space-separated aliases and case expressions', () => {
    type Rows = Result.ToRows<
      `
      select
        max(block_num) block,
        address token,
        sum(
          case
          when "from" = 0xB9621a707869d45A600acc2851418a1fe60500e7
          then -value
          when "to" = 0xB9621a707869d45A600acc2851418a1fe60500e7
          then value
          else 0
          end
        ) balance
      from transfer
      where (
        "to" = 0xB9621a707869d45A600acc2851418a1fe60500e7
        or "from" = 0xB9621a707869d45A600acc2851418a1fe60500e7
      )
      group by token
    `,
      ['event Transfer(address indexed from, address indexed to, uint256 value)']
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      block: string
      token: string
      balance: string
    }>()
  })

  test('behavior: handles quoted identifiers', () => {
    type Rows = Result.ToRows<
      `
      select
        log_idx,
        "from",
        "to",
        ids,
        amounts
      from transferbatch
      where block_num = 21258465
      and log_idx = 4
    `,
      [
        'event TransferBatch(address indexed operator, address indexed from, address indexed to, uint256 ids, uint256 amounts)',
      ]
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      log_idx: number
      from: `0x${string}`
      to: `0x${string}`
      ids: bigint
      amounts: bigint
    }>()
  })

  test('behavior: handles nested function calls with aliases', () => {
    type Rows = Result.ToRows<
      `
      select
        block_num,
        count(distinct(tx_hash)) transactions,
        count(distinct("from")) senders,
        count(distinct("to")) receivers
      from transfer
      group by block_num
      order by block_num desc
      limit 1
    `,
      ['event Transfer(address indexed from, address indexed to, uint256 value)']
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      block_num: bigint
      transactions: string
      senders: string
      receivers: string
    }>()
  })
  test('behavior: handles nested function calls with aliases', () => {
    type Rows = Result.ToRows<
      `
      select
        block_num,
        count(distinct(tx_hash)) transactions,
        count(distinct("from")) senders,
        count(distinct("to")) receivers
      from transfer
      group by block_num
      order by block_num desc
      limit 1
    `,
      ['event Transfer(address indexed from, address indexed to, uint256 value)']
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      block_num: bigint
      transactions: string
      senders: string
      receivers: string
    }>()
  })

  test('behavior: handles table-prefixed columns', () => {
    type Rows = Result.ToRows<
      `SELECT t1."to", t1.tokenId, t1.block_num
from transfer t1
left join transfer t2
on t1.tokenId = t2.tokenId
and t1.block_num < t2.block_num
and t1.address = t2.address
where t1.address = 0xE81b94b09B9dE001b75f2133A0Fb37346f7E8BA4
and t2.tokenId is null
`,
      ['event Transfer(address indexed from, address indexed to, uint256 value)']
    >

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      to: `0x${string}`
      tokenId: string
      block_num: bigint
    }>()
  })

  test('behavior: infers type for topic0 column', () => {
    type Rows = Result.ToRows<`
      select
        block_num,
        log_idx,
        tx_hash,
        address,
        topic0,
        topic1,
        data
      from logs
      where address = 0x1234567890123456789012345678901234567890
    `>

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      block_num: bigint
      log_idx: number
      tx_hash: `0x${string}`
      address: `0x${string}`
      topic0: `0x${string}`
      topic1: `0x${string}`
      data: `0x${string}`
    }>()
  })
})

describe('parse', () => {
  describe('txs table', () => {
    test('default', () => {
      const raw: Result.Raw = {
        ok: true,
        columns: [
          'block_num',
          'block_timestamp',
          'from',
          'gas_limit',
          'max_fee_per_gas',
          'hash',
          'idx',
          'input',
          'nonce',
          'to',
          'type',
          'value',
        ],
        rows: [
          [
            12345678,
            '2025-01-01T00:00:00+00:00',
            '0xabcdef1234567890abcdef1234567890abcdef01',
            21000,
            '1000000000',
            '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',
            0,
            '0xdeadbeef',
            42,
            '0xfedcba0987654321fedcba0987654321fedcba02',
            2,
            '1000000000000000000',
          ],
        ],
        row_count: 1,
      }

      const result = Result.parse(raw, {
        query:
          'select block_num, block_timestamp, "from", gas_limit, max_fee_per_gas, hash, idx, input, nonce, "to", type, value from txs',
      })

      expectTypeOf(result.rows).toExtend<
        readonly {
          block_num: bigint
          block_timestamp: number
          from: string
          gas_limit: bigint
          max_fee_per_gas: bigint
          hash: string
          idx: number
          input: string
          nonce: bigint
          to: string
          type: number
          value: bigint
        }[]
      >()

      expect(result).toMatchInlineSnapshot(`
        {
          "rows": [
            {
              "block_num": 12345678n,
              "block_timestamp": 1735689600,
              "from": "0xabcdef1234567890abcdef1234567890abcdef01",
              "gas_limit": 21000n,
              "hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
              "idx": 0,
              "input": "0xdeadbeef",
              "max_fee_per_gas": 1000000000n,
              "nonce": 42n,
              "to": "0xfedcba0987654321fedcba0987654321fedcba02",
              "type": 2,
              "value": 1000000000000000000n,
            },
          ],
        }
      `)
    })

    test('behavior: parses multiple rows', () => {
      const raw: Result.Raw = {
        ok: true,
        columns: ['hash'],
        rows: [
          ['0xabc123def456789012345678901234567890123456789012345678901234abcd'],
          ['0xdef456abc789012345678901234567890123456789012345678901234567efab'],
          ['0x789abc012345678901234567890123456789012345678901234567890123cdef'],
        ],
        row_count: 3,
      }

      const result = Result.parse(raw, {
        query: 'select hash from txs',
      })

      expect(result).toMatchInlineSnapshot(`
        {
          "rows": [
            {
              "hash": "0xabc123def456789012345678901234567890123456789012345678901234abcd",
            },
            {
              "hash": "0xdef456abc789012345678901234567890123456789012345678901234567efab",
            },
            {
              "hash": "0x789abc012345678901234567890123456789012345678901234567890123cdef",
            },
          ],
        }
      `)
    })
  })

  describe('logs table', () => {
    test('default', () => {
      const raw: Result.Raw = {
        ok: true,
        columns: [
          'address',
          'block_num',
          'block_timestamp',
          'data',
          'log_idx',
          'topic0',
          'topic1',
          'topic2',
          'tx_hash',
        ],
        rows: [
          [
            '0xc0ffee254729296a45a3885639ac7e10f9d54979',
            12345678,
            '2025-01-01T00:00:00+00:00',
            '0x00112233445566778899aabbccddeeff',
            5,
            '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
            '0x000000000000000000000000abc123def456789012345678901234567890abcd',
            '0x000000000000000000000000def456abc789012345678901234567890123efab',
            '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
          ],
        ],
        row_count: 1,
      }

      const result = Result.parse(raw, {
        query:
          'select address, block_num, block_timestamp, data, log_idx, topic0, topic1, topic2, tx_hash from logs',
      })

      expectTypeOf(result.rows).toExtend<
        readonly {
          address: `0x${string}`
          block_num: bigint
          block_timestamp: number
          data: `0x${string}`
          log_idx: number
          topic0: `0x${string}`
          topic1: `0x${string}`
          topic2: `0x${string}`
          tx_hash: `0x${string}`
        }[]
      >()

      expect(result).toMatchInlineSnapshot(`
        {
          "rows": [
            {
              "address": "0xc0ffee254729296a45a3885639ac7e10f9d54979",
              "block_num": 12345678n,
              "block_timestamp": 1735689600,
              "data": "0x00112233445566778899aabbccddeeff",
              "log_idx": 5,
              "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
              "topic1": "0x000000000000000000000000abc123def456789012345678901234567890abcd",
              "topic2": "0x000000000000000000000000def456abc789012345678901234567890123efab",
              "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            },
          ],
        }
      `)
    })
  })

  describe('blocks table', () => {
    test('default', () => {
      const raw: Result.Raw = {
        ok: true,
        columns: [
          'extra_data',
          'gas_limit',
          'gas_used',
          'hash',
          'miner',
          'num',
          'parent_hash',
          'timestamp',
          'timestamp_ms',
        ],
        rows: [
          [
            '0x496c6c756d696e61746544',
            30000000,
            21000,
            '0xaabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd',
            '0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5',
            12345678,
            '0x1122334455667788990011223344556677889900112233445566778899001122',
            '2025-01-01T00:00:00+00:00',
            1735689600000,
          ],
        ],
        row_count: 1,
      }

      const result = Result.parse(raw, {
        query:
          'select extra_data, gas_limit, gas_used, hash, miner, num, parent_hash, timestamp, timestamp_ms from blocks',
      })

      expectTypeOf(result.rows).toExtend<
        readonly {
          extra_data: `0x${string}`
          gas_limit: bigint
          gas_used: bigint
          hash: `0x${string}`
          miner: `0x${string}`
          num: bigint
          parent_hash: `0x${string}`
          timestamp: number
          timestamp_ms: bigint
        }[]
      >()

      expect(result).toMatchInlineSnapshot(`
        {
          "rows": [
            {
              "extra_data": "0x496c6c756d696e61746544",
              "gas_limit": 30000000n,
              "gas_used": 21000n,
              "hash": "0xaabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd",
              "miner": "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5",
              "num": 12345678n,
              "parent_hash": "0x1122334455667788990011223344556677889900112233445566778899001122",
              "timestamp": 1735689600,
              "timestamp_ms": 1735689600000n,
            },
          ],
        }
      `)
    })
  })

  describe('events table', () => {
    test('default', () => {
      const raw: Result.Raw = {
        ok: true,
        columns: [
          'block_num',
          'block_timestamp',
          'log_idx',
          'tx_hash',
          'sender',
          'receiver',
          'approved',
          'data',
          'hash',
          'hashes',
          'small',
          'medium',
          'exact',
          'amount',
          'balance',
          'total',
          'name',
          'symbol',
        ],
        rows: [
          [
            // logs table
            12345678,
            '2025-01-01T00:00:00+00:00',
            5,
            '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
            // address
            '0xabcdef1234567890abcdef1234567890abcdef01',
            '0xfedcba0987654321fedcba0987654321fedcba02',
            // bool
            true,
            // bytes
            '0xdeadbeef',
            '0xaabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd',
            // bytes array
            ['0xaabb', '0xccdd', '0xeeff'],
            // small int/uint
            '255',
            '65535',
            '281474976710655',
            // large int/uint
            '1000000000000000000',
            '999999999999999999999',
            '115792089237316195423570985008687907853269984665640564039457584007913129639935',
            // string
            'Ethereum',
            'ETH',
          ],
        ],
        row_count: 1,
      }

      const result = Result.parse(raw, {
        query:
          'select address, block_num, block_timestamp, log_idx, tx_hash, sender, receiver, approved, data, hash, hashes, small, medium, exact, amount, balance, total, name, symbol from alltypes',
        signatures: [
          'event AllTypes(address indexed sender, address indexed receiver, bool approved, bytes data, bytes32 hash, bytes32[] hashes, uint8 small, uint16 medium, uint48 exact, uint64 amount, uint128 balance, uint256 total, string name, string symbol)',
        ],
      })

      expectTypeOf(result.rows).toExtend<
        readonly {
          address: `0x${string}`
          block_num: bigint
          block_timestamp: number
          log_idx: number
          tx_hash: `0x${string}`
          sender: `0x${string}`
          receiver: `0x${string}`
          approved: boolean
          data: `0x${string}`
          hash: `0x${string}`
          hashes: readonly `0x${string}`[]
          small: number
          medium: number
          exact: number
          amount: bigint
          balance: bigint
          total: bigint
          name: string
          symbol: string
        }[]
      >()

      expect(result).toMatchInlineSnapshot(`
        {
          "rows": [
            {
              "amount": 1000000000000000000n,
              "approved": true,
              "balance": 999999999999999999999n,
              "block_num": 12345678n,
              "block_timestamp": 1735689600,
              "data": "0xdeadbeef",
              "exact": 281474976710655,
              "hash": "0xaabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd",
              "hashes": [
                "0xaabb",
                "0xccdd",
                "0xeeff",
              ],
              "log_idx": 5,
              "medium": 65535,
              "name": "Ethereum",
              "receiver": "0xfedcba0987654321fedcba0987654321fedcba02",
              "sender": "0xabcdef1234567890abcdef1234567890abcdef01",
              "small": 255,
              "symbol": "ETH",
              "total": 115792089237316195423570985008687907853269984665640564039457584007913129639935n,
              "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            },
          ],
        }
      `)
    })
  })

  test('behavior: table prefix handling', () => {
    const raw: Result.Raw = {
      ok: true,
      columns: ['hash'],
      rows: [['0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890']],
      row_count: 1,
    }

    const result = Result.parse(raw, {
      query: 'select txs.hash from txs',
    })

    expect(result).toMatchInlineSnapshot(`
      {
        "rows": [
          {
            "hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
          },
        ],
      }
    `)
  })

  test('behavior: non-standard table', () => {
    const raw: Result.Raw = {
      ok: true,
      columns: ['foo', 'bar'],
      rows: [['hello', 123]],
      row_count: 1,
    }

    // Use type assertion since non-standard tables require signatures at type level
    const result = Result.parse(raw, {
      query: 'select foo, bar from custom_table' as string,
    })

    expect(result).toMatchInlineSnapshot(`
      {
        "rows": [
          {
            "bar": 123,
            "foo": "hello",
          },
        ],
      }
    `)
  })

  test('behavior: empty results', () => {
    const raw: Result.Raw = {
      ok: true,
      columns: ['hash'],
      rows: [],
      row_count: 0,
    }

    const result = Result.parse(raw, {
      query: 'select hash from txs',
    })

    expect(result).toMatchInlineSnapshot(`
      {
        "rows": [],
      }
    `)
  })
})
