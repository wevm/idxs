import { describe, expect, expectTypeOf, test } from 'vitest'
import * as Result from './result.js'

describe('ToRows', () => {
  test('default', () => {
    type Rows = Result.ToRows<
      `select "from", "to", tokens from transfer where chain = 8453 limit 3`,
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

  test('behavior: infers array type for topics column', () => {
    type Rows = Result.ToRows<`
      select
        block_num,
        log_idx,
        tx_hash,
        address,
        topics,
        data
      from logs
      where address = 0x1234567890123456789012345678901234567890
    `>

    expectTypeOf<Rows[number]>().toEqualTypeOf<{
      block_num: bigint
      log_idx: number
      tx_hash: `0x${string}`
      address: `0x${string}`
      topics: `0x${string}`[]
      data: `0x${string}`
    }>()
  })
})

describe('parse', () => {
  describe('txs table', () => {
    test('default', () => {
      const raw: Result.Raw = {
        cursor: '8453-123',
        columns: [
          { name: 'block_num', pgtype: 'int8' },
          { name: 'block_timestamp', pgtype: 'timestamptz' },
          { name: 'chain', pgtype: 'int8' },
          { name: 'from', pgtype: 'bytea' },
          { name: 'gas', pgtype: 'numeric' },
          { name: 'gas_price', pgtype: 'numeric' },
          { name: 'hash', pgtype: 'bytea' },
          { name: 'idx', pgtype: 'int4' },
          { name: 'input', pgtype: 'bytea' },
          { name: 'nonce', pgtype: 'bytea' },
          { name: 'to', pgtype: 'bytea' },
          { name: 'type', pgtype: 'int2' },
          { name: 'value', pgtype: 'numeric' },
        ],
        rows: [
          [
            12345678,
            '2023-06-15 0:35:51.0 +00:00:00',
            8453,
            '0xabcdef1234567890abcdef1234567890abcdef01',
            '21000',
            '1000000000',
            '0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef',
            0,
            '0xdeadbeef',
            '42',
            '0xfedcba0987654321fedcba0987654321fedcba02',
            2,
            '1000000000000000000',
          ],
        ],
      }

      const result = Result.parse(raw, {
        query:
          'select block_num, block_timestamp, chain, "from", gas, gas_price, hash, idx, input, nonce, "to", type, value from txs',
      })

      expectTypeOf(result.cursor).toEqualTypeOf<string>()
      expectTypeOf(result.rows).toExtend<
        readonly {
          block_num: bigint
          block_timestamp: number
          chain: number
          from: string
          gas: bigint
          gas_price: bigint
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
          "cursor": "8453-123",
          "rows": [
            {
              "block_num": 12345678n,
              "block_timestamp": 1686789351,
              "chain": 8453,
              "from": "0xabcdef1234567890abcdef1234567890abcdef01",
              "gas": 21000n,
              "gas_price": 1000000000n,
              "hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
              "idx": 0,
              "input": "0xdeadbeef",
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
        cursor: '8453-123',
        columns: [
          { name: 'chain', pgtype: 'int8' },
          { name: 'hash', pgtype: 'bytea' },
        ],
        rows: [
          [8453, '0xabc123def456789012345678901234567890123456789012345678901234abcd'],
          [8453, '0xdef456abc789012345678901234567890123456789012345678901234567efab'],
          [8453, '0x789abc012345678901234567890123456789012345678901234567890123cdef'],
        ],
      }

      const result = Result.parse(raw, {
        query: 'select chain, hash from txs',
      })

      expect(result).toMatchInlineSnapshot(`
        {
          "cursor": "8453-123",
          "rows": [
            {
              "chain": 8453,
              "hash": "0xabc123def456789012345678901234567890123456789012345678901234abcd",
            },
            {
              "chain": 8453,
              "hash": "0xdef456abc789012345678901234567890123456789012345678901234567efab",
            },
            {
              "chain": 8453,
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
        cursor: '8453-123',
        columns: [
          { name: 'address', pgtype: 'bytea' },
          { name: 'block_num', pgtype: 'int8' },
          { name: 'block_timestamp', pgtype: 'timestamptz' },
          { name: 'chain', pgtype: 'int8' },
          { name: 'data', pgtype: 'bytea' },
          { name: 'log_idx', pgtype: 'int4' },
          { name: 'topics', pgtype: 'bytea[]' },
          { name: 'tx_hash', pgtype: 'bytea' },
        ],
        rows: [
          [
            '0xc0ffee254729296a45a3885639ac7e10f9d54979',
            12345678,
            '2023-06-15 0:35:51.0 +00:00:00',
            8453,
            '0x00112233445566778899aabbccddeeff',
            5,
            [
              '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef',
              '0x000000000000000000000000abc123def456789012345678901234567890abcd',
              '0x000000000000000000000000def456abc789012345678901234567890123efab',
            ],
            '0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890',
          ],
        ],
      }

      const result = Result.parse(raw, {
        query:
          'select address, block_num, block_timestamp, chain, data, log_idx, topics, tx_hash from logs',
      })

      expectTypeOf(result.cursor).toEqualTypeOf<string>()
      expectTypeOf(result.rows).toExtend<
        readonly {
          address: `0x${string}`
          block_num: bigint
          block_timestamp: number
          chain: number
          data: `0x${string}`
          log_idx: number
          topics: `0x${string}`[]
          tx_hash: `0x${string}`
        }[]
      >()

      expect(result).toMatchInlineSnapshot(`
        {
          "cursor": "8453-123",
          "rows": [
            {
              "address": "0xc0ffee254729296a45a3885639ac7e10f9d54979",
              "block_num": 12345678n,
              "block_timestamp": 1686789351,
              "chain": 8453,
              "data": "0x00112233445566778899aabbccddeeff",
              "log_idx": 5,
              "topics": [
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000abc123def456789012345678901234567890abcd",
                "0x000000000000000000000000def456abc789012345678901234567890123efab",
              ],
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
        cursor: '8453-123',
        columns: [
          { name: 'chain', pgtype: 'int8' },
          { name: 'extra_data', pgtype: 'bytea' },
          { name: 'gas_limit', pgtype: 'numeric' },
          { name: 'gas_used', pgtype: 'numeric' },
          { name: 'hash', pgtype: 'bytea' },
          { name: 'miner', pgtype: 'bytea' },
          { name: 'nonce', pgtype: 'bytea' },
          { name: 'num', pgtype: 'int8' },
          { name: 'receipts_root', pgtype: 'bytea' },
          { name: 'size', pgtype: 'int4' },
          { name: 'state_root', pgtype: 'bytea' },
          { name: 'timestamp', pgtype: 'int8' },
        ],
        rows: [
          [
            8453,
            '0x496c6c756d696e61746544',
            30000000n,
            21000n,
            '0xaabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd',
            '0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5',
            '0',
            12345678n,
            '0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421',
            1234,
            '0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544',
            1686789351,
          ],
        ],
      }

      const result = Result.parse(raw, {
        query:
          'select chain, extra_data, gas_limit, gas_used, hash, miner, nonce, num, receipts_root, size, state_root, timestamp from blocks',
      })

      expectTypeOf(result.cursor).toEqualTypeOf<string>()
      expectTypeOf(result.rows).toExtend<
        readonly {
          chain: number
          extra_data: `0x${string}`
          gas_limit: bigint
          gas_used: bigint
          hash: `0x${string}`
          miner: `0x${string}`
          nonce: bigint
          num: bigint
          receipts_root: `0x${string}`
          size: number
          state_root: `0x${string}`
          timestamp: number
        }[]
      >()

      expect(result).toMatchInlineSnapshot(`
        {
          "cursor": "8453-123",
          "rows": [
            {
              "chain": 8453,
              "extra_data": "0x496c6c756d696e61746544",
              "gas_limit": 30000000n,
              "gas_used": 21000n,
              "hash": "0xaabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd",
              "miner": "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5",
              "nonce": 0n,
              "num": 12345678n,
              "receipts_root": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
              "size": 1234,
              "state_root": "0xd7f8974fb5ac78d9ac099b9ad5018bedc2ce0a72dad1827a1709da30580f0544",
              "timestamp": 1686789351,
            },
          ],
        }
      `)
    })
  })

  describe('events table', () => {
    test('default', () => {
      const raw: Result.Raw = {
        cursor: '8453-123',
        columns: [
          // logs table types
          { name: 'chain', pgtype: 'int8' },
          { name: 'block_num', pgtype: 'int8' },
          { name: 'block_timestamp', pgtype: 'timestamptz' },
          { name: 'log_idx', pgtype: 'int4' },
          { name: 'tx_hash', pgtype: 'bytea' },
          // address types
          { name: 'sender', pgtype: 'bytea' },
          { name: 'receiver', pgtype: 'bytea' },
          // bool type
          { name: 'approved', pgtype: 'bool' },
          // bytes types
          { name: 'data', pgtype: 'bytea' },
          { name: 'hash', pgtype: 'bytea' },
          // bytes array
          { name: 'hashes', pgtype: 'bytea[]' },
          // small int/uint (size <= 48) -> number
          { name: 'small', pgtype: 'int4' },
          { name: 'medium', pgtype: 'int4' },
          { name: 'exact', pgtype: 'int8' },
          // large int/uint (size > 48) -> bigint
          { name: 'amount', pgtype: 'numeric' },
          { name: 'balance', pgtype: 'numeric' },
          { name: 'total', pgtype: 'numeric' },
          // string type
          { name: 'name', pgtype: 'text' },
          { name: 'symbol', pgtype: 'text' },
        ],
        rows: [
          [
            // logs table
            8453,
            12345678,
            '2023-06-15 0:35:51.0 +00:00:00',
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
      }

      const result = Result.parse(raw, {
        query:
          'select address, chain, block_num, block_timestamp, log_idx, tx_hash, sender, receiver, approved, data, hash, hashes, small, medium, exact, amount, balance, total, name, symbol from alltypes',
        signatures: [
          'event AllTypes(address indexed sender, address indexed receiver, bool approved, bytes data, bytes32 hash, bytes32[] hashes, uint8 small, uint16 medium, uint48 exact, uint64 amount, uint128 balance, uint256 total, string name, string symbol)',
        ],
      })

      expectTypeOf(result.cursor).toEqualTypeOf<string>()
      expectTypeOf(result.rows).toExtend<
        readonly {
          address: `0x${string}`
          chain: number
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
          "cursor": "8453-123",
          "rows": [
            {
              "amount": 1000000000000000000n,
              "approved": true,
              "balance": 999999999999999999999n,
              "block_num": 12345678n,
              "block_timestamp": 1686789351,
              "chain": 8453,
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
      cursor: '8453-123',
      columns: [{ name: 'hash', pgtype: 'bytea' }],
      rows: [['0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890']],
    }

    const result = Result.parse(raw, {
      query: 'select txs.hash from txs',
    })

    expect(result).toMatchInlineSnapshot(`
      {
        "cursor": "8453-123",
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
      cursor: '8453-123',
      columns: [
        { name: 'foo', pgtype: 'text' },
        { name: 'bar', pgtype: 'int8' },
      ],
      rows: [['hello', 123]],
    }

    // Use type assertion since non-standard tables require signatures at type level
    const result = Result.parse(raw, {
      query: 'select foo, bar from custom_table' as string,
    })

    expect(result).toMatchInlineSnapshot(`
        {
          "cursor": "8453-123",
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
      cursor: '8453-123',
      columns: [{ name: 'chain', pgtype: 'int8' }],
      rows: [],
    }

    const result = Result.parse(raw, {
      query: 'select chain from txs',
    })

    expect(result).toMatchInlineSnapshot(`
        {
          "cursor": "8453-123",
          "rows": [],
        }
      `)
  })
})
