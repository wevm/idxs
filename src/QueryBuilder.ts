import {
  type Abi,
  type AbiParameter,
  type AbiParameterToPrimitiveType,
  formatAbi,
  type ParseAbi,
  parseAbiItem,
} from 'abitype'
import type {
  CompiledQuery,
  DatabaseConnection,
  Driver as kysely_Driver,
  QueryResult,
} from 'kysely'
import { Kysely, PostgresAdapter, PostgresIntrospector, PostgresQueryCompiler } from 'kysely'
import type * as IS from './IndexSupply.js'
import type { StandardColumnTypes } from './internal/result.js'

declare module 'kysely' {
  // @ts-expect-error
  interface SelectQueryBuilder<O> {
    execute(): Promise<O[] & { cursor: string }>
  }
}

/**
 * Standard Index Supply EVM tables.
 */
export namespace Tables {
  /** Blocks table containing blockchain block data. */
  export type Blocks = {
    /** Chain ID. */
    chain: StandardColumnTypes['chain']
    /** Block number. */
    num: StandardColumnTypes['block_num']
    /** Block timestamp. */
    timestamp: StandardColumnTypes['block_timestamp']
    /** Block size in bytes. */
    size: StandardColumnTypes['size']
    /** Gas limit for the block. */
    gas_limit: StandardColumnTypes['gas_limit']
    /** Gas used in the block. */
    gas_used: StandardColumnTypes['gas_used']
    /** Block nonce. */
    nonce: StandardColumnTypes['nonce']
    /** Block hash. */
    hash: StandardColumnTypes['hash']
    /** Receipts root hash. */
    receipts_root: StandardColumnTypes['receipts_root']
    /** State root hash. */
    state_root: StandardColumnTypes['state_root']
    /** Extra data. */
    extra_data: StandardColumnTypes['extra_data']
    /** Miner address. */
    miner: StandardColumnTypes['miner']
  }

  /** Transactions table containing transaction data. */
  export type Txs = {
    /** Chain ID. */
    chain: StandardColumnTypes['chain']
    /** Block number. */
    block_num: StandardColumnTypes['block_num']
    /** Block timestamp. */
    block_timestamp: StandardColumnTypes['block_timestamp']
    /** Transaction index in block. */
    idx: StandardColumnTypes['idx']
    /** Transaction type. */
    type: StandardColumnTypes['type']
    /** Gas limit. */
    gas: StandardColumnTypes['gas']
    /** Gas price. */
    gas_price: StandardColumnTypes['gas_price']
    /** Transaction nonce. */
    nonce: StandardColumnTypes['nonce']
    /** Transaction hash. */
    hash: StandardColumnTypes['hash']
    /** Sender address. */
    from: StandardColumnTypes['from']
    /** Recipient address. */
    to: StandardColumnTypes['to']
    /** Transaction input data. */
    input: StandardColumnTypes['input']
    /** Transaction value. */
    value: StandardColumnTypes['value']
  }

  /** Logs table containing event log data. */
  export type Logs = {
    /** Chain ID. */
    chain: StandardColumnTypes['chain']
    /** Block number. */
    block_num: StandardColumnTypes['block_num']
    /** Block timestamp. */
    block_timestamp: StandardColumnTypes['block_timestamp']
    /** Log index in block. */
    log_idx: StandardColumnTypes['log_idx']
    /** Transaction hash. */
    tx_hash: StandardColumnTypes['tx_hash']
    /** Contract address that emitted the log. */
    address: StandardColumnTypes['address']
    /** Event topics (indexed parameters). */
    topics: StandardColumnTypes['topics']
    /** Event data (non-indexed parameters). */
    data: StandardColumnTypes['data']
  }
}

/**
 * Database schema type containing all standard Index Supply tables.
 */
export type Database = {
  blocks: Tables.Blocks
  txs: Tables.Txs
  logs: Tables.Logs
}

export type QueryBuilder<rootAbi extends Abi | undefined = undefined> = Kysely<
  // biome-ignore lint/complexity/noBannedTypes: _
  Database & (rootAbi extends Abi ? AbiToDatabase<rootAbi> : {})
> & {
  /**
   * Sets the cursor position for pagination.
   *
   * @example
   * ```ts
   * const qb = QueryBuilder.from(is).atCursor('1-12345')
   *
   * // Or with object notation
   * const qb = QueryBuilder.from(is).atCursor({ chainId: 1, blockNumber: 12345 })
   * ```
   *
   * @param cursor - The cursor to start from.
   * @returns A new QueryBuilder instance with the cursor set.
   */
  atCursor: (cursor: QueryBuilder.Cursor) => Omit<QueryBuilder<rootAbi>, 'cursor'>
  /**
   * Adds ABI definitions to enable querying custom event/function tables.
   *
   * @param abi - The ABI array to add.
   * @returns A new QueryBuilder instance with the ABI added.
   *
   * @example
   * ```ts
   * const abi = [{ type: 'event', name: 'Transfer', inputs: [...] }] as const
   * const qb = QueryBuilder.from(is).withAbi(abi)
   * ```
   */
  withAbi: <const abi extends Abi>(
    abi: abi,
  ) => QueryBuilder<rootAbi extends Abi ? [...rootAbi, ...abi] : abi>
  /**
   * Adds human-readable signatures to enable querying custom event/function tables.
   *
   * @param signatures - Array of human-readable event/function signatures.
   * @returns A new QueryBuilder instance with the signatures added.
   *
   * @example
   * ```ts
   * const qb = QueryBuilder.from(is).withSignatures([
   *   'event Transfer(address indexed from, address indexed to, uint256 value)',
   *   'event Approval(address indexed owner, address indexed spender, uint256 value)',
   * ])
   *
   * const transfers = await qb
   *   .selectFrom('transfer')
   *   .select(['from', 'to', 'value'])
   *   .execute()
   * ```
   */
  withSignatures: <const signatures extends readonly string[]>(
    signatures: signatures,
  ) => QueryBuilder<
    rootAbi extends Abi ? [...rootAbi, ...ParseAbi<signatures>] : ParseAbi<signatures>
  >
}

export declare namespace QueryBuilder {
  /** Cursor type for pagination. Can be a string or an object with `chainId` and `blockNumber`. */
  export type Cursor = IS.Cursor
}

/**
 * Creates a [Kysely-based](https://kysely.dev) QueryBuilder instance from an Index Supply client.
 *
 * @example
 * ```ts
 * import { IndexSupply, QueryBuilder } from 'idxs'
 *
 * const is = IndexSupply.create({ apiKey: 'your-api-key' })
 * const qb = QueryBuilder.from(is)
 *
 * // Query transactions
 * const txs = await qb
 *   .selectFrom('txs')
 *   .select(['hash', 'from', 'to', 'value'])
 *   .where('chain', '=', 1)
 *   .limit(10)
 *   .execute()
 *
 * console.log(txs) // [{ hash: '0x...', from: '0x...', to: '0x...', value: '...' }, ...]
 * console.log(txs.cursor) // '1-12345678'
 * ```
 *
 * @example
 * ```ts
 * // Query with event signatures
 * const transfers = await qb
 *   .withSignatures(['event Transfer(address indexed from, address indexed to, uint256 value)'])
 *   .selectFrom('transfer')
 *   .select(['from', 'to', 'value'])
 *   .where('chain', '=', 1)
 *   .limit(100)
 *   .execute()
 * ```
 *
 * @example
 * ```ts
 * // Pagination with cursor
 *
 * let qb = QueryBuilder.from(is)
 *
 * let cursor: string | undefined
 * while (true) {
 *   if (cursor) qb = qb.atCursor(cursor)
 *   const txs = await qb.selectFrom('txs').select(['hash']).limit(100).execute()
 *   if (txs.length === 0) break
 *   cursor = txs.cursor
 *   // Process txs...
 * }
 * ```
 *
 * @param options - The Index Supply client instance (must have `fetch` and `live` methods).
 * @returns A new Kysely-based QueryBuilder instance.
 */
export function from(options: from.Options): QueryBuilder {
  let cursor: QueryBuilder.Cursor | undefined
  const signatures: string[] = []

  function inner(
    o: {
      cursor?: QueryBuilder.Cursor | undefined
      signatures?: readonly string[] | undefined
    } = {},
  ) {
    cursor ??= o.cursor
    signatures.push(...(o.signatures ?? []))

    const kysely = new Kysely({
      dialect: {
        createAdapter: () => new PostgresAdapter(),
        createDriver: () => new Driver({ ...options, cursor, signatures }),
        createIntrospector: (db) => new PostgresIntrospector(db),
        createQueryCompiler: () => new PostgresQueryCompiler(),
      },
    })

    return {
      atCursor(cursor: QueryBuilder.Cursor) {
        return inner({ cursor }) as never
      },
      selectFrom: kysely.selectFrom.bind(kysely),
      withAbi(abi: Abi) {
        return inner({ signatures: formatAbi(abi) }) as never
      },
      withSignatures(signatures: readonly string[]) {
        return inner({ signatures }) as never
      },
    }
  }

  return inner() as never
}

export namespace from {
  /** Options for creating a QueryBuilder. */
  export type Options = Pick<IS.IS, 'fetch' | 'live'>

  /** Return type of the `from` function. */
  export type ReturnValue = QueryBuilder
}

/** @internal */
class Driver implements kysely_Driver {
  constructor(
    private options: from.Options & {
      cursor?: QueryBuilder.Cursor | undefined
      signatures?: string[] | undefined
    },
  ) {}

  async init(): Promise<void> {
    // Noop
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    return new Connection(this.options)
  }

  async beginTransaction(): Promise<void> {
    throw new Error('Transactions are not supported')
  }

  async commitTransaction(): Promise<void> {
    throw new Error('Transactions are not supported')
  }

  async rollbackTransaction(): Promise<void> {
    throw new Error('Transactions are not supported')
  }

  async releaseConnection(): Promise<void> {
    // Noop
  }

  async destroy(): Promise<void> {
    // Noop
  }
}

/** @internal */
class Connection implements DatabaseConnection {
  constructor(
    private options: from.Options & {
      cursor?: QueryBuilder.Cursor | undefined
      signatures?: string[] | undefined
    },
  ) {}

  async executeQuery<row>(compiledQuery: CompiledQuery): Promise<QueryResult<row>> {
    const { query, signatures } = this.prepareQuery(compiledQuery)

    const result = await this.options.fetch({
      cursor: this.options.cursor,
      query,
      signatures: signatures as readonly IS.Signature[],
    })

    return this.parseResult(result)
  }

  async *streamQuery<row>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<row>> {
    const { query, signatures } = this.prepareQuery(compiledQuery)

    // Use the live streaming endpoint
    for await (const result of this.options.live({
      cursor: this.options.cursor,
      query,
      signatures: signatures as readonly IS.Signature[],
    }))
      yield this.parseResult<row>(result)
  }

  parseResult<row>(
    result: IS.IS.fetch.ReturnValue<string, readonly IS.Signature[]>,
  ): QueryResult<row> {
    const rows = result.rows

    Object.defineProperty(rows, 'cursor', {
      value: result.cursor,
      enumerable: false,
      writable: false,
    })

    return { rows } as unknown as QueryResult<row>
  }

  prepareQuery(compiledQuery: CompiledQuery) {
    let query = compiledQuery.sql

    compiledQuery.parameters.forEach((param, i) => {
      const placeholder = `$${i + 1}`
      query = query.replaceAll(placeholder, String(param))
    })

    const signatures: string[] = []

    for (const signature of this.options.signatures ?? []) {
      const abiItem = parseAbiItem(signature)
      if (!('name' in abiItem)) continue
      const signatureName = abiItem.name.toLowerCase()
      const regex = new RegExp(`(from|join) "(${signatureName})"`, 'gi')
      const matches = regex.test(query)
      if (!matches) continue
      query = query.replace(regex, `$1 $2`)
      signatures.push(signature)
    }

    return { query, signatures }
  }
}

/** @internal */
type RemoveDuplicates<abi extends Abi, seen extends string = never> = abi extends readonly [
  infer first,
  ...infer rest extends Abi,
]
  ? first extends { name: infer name extends string }
    ? Lowercase<name> extends seen
      ? RemoveDuplicates<rest, seen>
      : readonly [first, ...RemoveDuplicates<rest, seen | Lowercase<name>>]
    : RemoveDuplicates<rest, seen>
  : readonly []

/** @internal */
type AbiToDatabase<
  abiOrSignatures extends Abi | readonly string[],
  ///
  abi extends Abi = RemoveDuplicates<
    abiOrSignatures extends readonly string[] ? ParseAbi<abiOrSignatures> : abiOrSignatures
  >,
> = {
  [key in abi[number] as key extends {
    name: infer name extends string
  }
    ? Lowercase<name>
    : never]: key extends {
    inputs: infer inputs extends readonly AbiParameter[]
    type: 'function' | 'event'
  }
    ? AbiParameterToPrimitiveType<{
        components: inputs
        type: 'tuple'
      }> &
        Tables.Logs
    : never
}
