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
import type { StandardColumnTypes } from './internal/result.js'
import type * as Tidx from './Tidx.js'

declare module 'kysely' {
  // @ts-expect-error
  interface SelectQueryBuilder<O> {
    execute(): Promise<O[]>
  }
}

/**
 * Standard tidx EVM tables.
 */
export namespace Tables {
  /** Blocks table containing blockchain block data. */
  export type Blocks = {
    /** Block number. */
    num: StandardColumnTypes['block_num']
    /** Block hash. */
    hash: StandardColumnTypes['hash']
    /** Parent block hash. */
    parent_hash: StandardColumnTypes['parent_hash']
    /** Block timestamp. */
    timestamp: StandardColumnTypes['block_timestamp']
    /** Block timestamp in milliseconds. */
    timestamp_ms: StandardColumnTypes['timestamp_ms']
    /** Gas limit for the block. */
    gas_limit: StandardColumnTypes['gas_limit']
    /** Gas used in the block. */
    gas_used: StandardColumnTypes['gas_used']
    /** Miner address. */
    miner: StandardColumnTypes['miner']
    /** Extra data. */
    extra_data: StandardColumnTypes['extra_data']
  }

  /** Transactions table containing transaction data. */
  export type Txs = {
    /** Block number. */
    block_num: StandardColumnTypes['block_num']
    /** Block timestamp. */
    block_timestamp: StandardColumnTypes['block_timestamp']
    /** Transaction index in block. */
    idx: StandardColumnTypes['idx']
    /** Transaction hash. */
    hash: StandardColumnTypes['hash']
    /** Transaction type. */
    type: StandardColumnTypes['type']
    /** Sender address. */
    from: StandardColumnTypes['from']
    /** Recipient address. */
    to: StandardColumnTypes['to']
    /** Transaction value. */
    value: StandardColumnTypes['value']
    /** Transaction input data. */
    input: StandardColumnTypes['input']
    /** Gas limit. */
    gas_limit: StandardColumnTypes['gas_limit']
    /** Max fee per gas. */
    max_fee_per_gas: StandardColumnTypes['max_fee_per_gas']
    /** Max priority fee per gas. */
    max_priority_fee_per_gas: StandardColumnTypes['max_priority_fee_per_gas']
    /** Gas used. */
    gas_used: StandardColumnTypes['gas_used']
    /** Nonce key. */
    nonce_key: StandardColumnTypes['nonce_key']
    /** Transaction nonce. */
    nonce: StandardColumnTypes['nonce']
    /** Fee token address. */
    fee_token: StandardColumnTypes['fee_token']
    /** Fee payer address. */
    fee_payer: StandardColumnTypes['fee_payer']
    /** Internal calls. */
    calls: StandardColumnTypes['calls']
    /** Number of internal calls. */
    call_count: StandardColumnTypes['call_count']
    /** Valid before timestamp. */
    valid_before: StandardColumnTypes['valid_before']
    /** Valid after timestamp. */
    valid_after: StandardColumnTypes['valid_after']
    /** Signature type. */
    signature_type: StandardColumnTypes['signature_type']
  }

  /** Logs table containing event log data. */
  export type Logs = {
    /** Block number. */
    block_num: StandardColumnTypes['block_num']
    /** Block timestamp. */
    block_timestamp: StandardColumnTypes['block_timestamp']
    /** Log index in block. */
    log_idx: StandardColumnTypes['log_idx']
    /** Transaction index in block. */
    tx_idx: StandardColumnTypes['tx_idx']
    /** Transaction hash. */
    tx_hash: StandardColumnTypes['tx_hash']
    /** Contract address that emitted the log. */
    address: StandardColumnTypes['address']
    /** Event selector. */
    selector: StandardColumnTypes['selector']
    /** Topic 0. */
    topic0: StandardColumnTypes['topic0']
    /** Topic 1. */
    topic1: StandardColumnTypes['topic1']
    /** Topic 2. */
    topic2: StandardColumnTypes['topic2']
    /** Topic 3. */
    topic3: StandardColumnTypes['topic3']
    /** Event data (non-indexed parameters). */
    data: StandardColumnTypes['data']
  }

  /** Receipts table containing transaction receipt data. */
  export type Receipts = {
    /** Block number. */
    block_num: StandardColumnTypes['block_num']
    /** Block timestamp. */
    block_timestamp: StandardColumnTypes['block_timestamp']
    /** Transaction index in block. */
    tx_idx: StandardColumnTypes['tx_idx']
    /** Transaction hash. */
    tx_hash: StandardColumnTypes['tx_hash']
    /** Sender address. */
    from: StandardColumnTypes['from']
    /** Recipient address. */
    to: StandardColumnTypes['to']
    /** Contract address created. */
    contract_address: StandardColumnTypes['contract_address']
    /** Gas used. */
    gas_used: StandardColumnTypes['gas_used']
    /** Cumulative gas used. */
    cumulative_gas_used: StandardColumnTypes['cumulative_gas_used']
    /** Effective gas price. */
    effective_gas_price: StandardColumnTypes['effective_gas_price']
    /** Transaction status. */
    status: StandardColumnTypes['status']
    /** Fee payer address. */
    fee_payer: StandardColumnTypes['fee_payer']
  }
}

/**
 * Database schema type containing all standard tidx tables.
 */
export type Database = {
  blocks: Tables.Blocks
  txs: Tables.Txs
  logs: Tables.Logs
  receipts: Tables.Receipts
}

export type QueryBuilder<rootAbi extends Abi | undefined = undefined> = Kysely<
  // biome-ignore lint/complexity/noBannedTypes: _
  Database & (rootAbi extends Abi ? AbiToDatabase<rootAbi> : {})
> & {
  /**
   * Adds ABI definitions to enable querying custom event/function tables.
   *
   * @param abi - The ABI array to add.
   * @returns A new QueryBuilder instance with the ABI added.
   *
   * @example
   * ```ts
   * const abi = [{ type: 'event', name: 'Transfer', inputs: [...] }] as const
   * const qb = QueryBuilder.from(tidx).withAbi(abi)
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
   * const qb = QueryBuilder.from(tidx).withSignatures([
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

/**
 * Creates a [Kysely-based](https://kysely.dev) QueryBuilder instance from a tidx client.
 *
 * @example
 * ```ts
 * import { Tidx, QueryBuilder } from 'tidx'
 *
 * const tidx = Tidx.create({ basicAuth: 'your-api-key', chainId: 1 })
 * const qb = QueryBuilder.from(tidx)
 *
 * // Query transactions
 * const txs = await qb
 *   .selectFrom('txs')
 *   .select(['hash', 'from', 'to', 'value'])
 *   .limit(10)
 *   .execute()
 *
 * console.log(txs) // [{ hash: '0x...', from: '0x...', to: '0x...', value: '...' }, ...]
 * ```
 *
 * @example
 * ```ts
 * // Query with event signatures
 * const transfers = await qb
 *   .withSignatures(['event Transfer(address indexed from, address indexed to, uint256 value)'])
 *   .selectFrom('transfer')
 *   .select(['from', 'to', 'value'])
 *   .limit(100)
 *   .execute()
 * ```
 *
 * @param options - Options including the tidx client (`fetch` and `live` methods) and `chainId`.
 * @returns A new Kysely-based QueryBuilder instance.
 */
export function from(options: from.Options): QueryBuilder {
  const signatures: string[] = []

  function inner(o: { signatures?: readonly string[] | undefined } = {}) {
    signatures.push(...(o.signatures ?? []))

    const kysely = new Kysely({
      dialect: {
        createAdapter: () => new PostgresAdapter(),
        createDriver: () => new Driver({ ...options, signatures }),
        createIntrospector: (db) => new PostgresIntrospector(db),
        createQueryCompiler: () => new PostgresQueryCompiler(),
      },
    })

    return {
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
  export type Options = Pick<Tidx.Tidx, 'fetch' | 'live'> & {
    chainId?: number | undefined
    engine?: string | undefined
  }

  /** Return type of the `from` function. */
  export type ReturnValue = QueryBuilder
}

/** @internal */
class Driver implements kysely_Driver {
  constructor(
    private options: from.Options & {
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
      signatures?: string[] | undefined
    },
  ) {}

  async executeQuery<row>(compiledQuery: CompiledQuery): Promise<QueryResult<row>> {
    const { query, signatures } = this.prepareQuery(compiledQuery)

    const result = await this.options.fetch({
      chainId: this.options.chainId,
      engine: this.options.engine,
      query,
      signatures: signatures as readonly Tidx.Signature[],
    })

    return this.parseResult(result)
  }

  async *streamQuery<row>(compiledQuery: CompiledQuery): AsyncIterableIterator<QueryResult<row>> {
    const { query, signatures } = this.prepareQuery(compiledQuery)

    // Use the live streaming endpoint
    for await (const result of this.options.live({
      chainId: this.options.chainId,
      engine: this.options.engine,
      query,
      signatures: signatures as readonly Tidx.Signature[],
    }))
      yield this.parseResult<row>(result)
  }

  parseResult<row>(
    result: Tidx.Tidx.fetch.ReturnValue<string, readonly Tidx.Signature[]>,
  ): QueryResult<row> {
    const rows = result.rows
    return { rows } as unknown as QueryResult<row>
  }

  prepareQuery(compiledQuery: CompiledQuery) {
    let query = compiledQuery.sql

    for (let i = compiledQuery.parameters.length - 1; i >= 0; i--) {
      const placeholder = `$${i + 1}`
      const param = compiledQuery.parameters[i]
      const value = typeof param === 'string' ? `'${param}'` : String(param)
      query = query.replaceAll(placeholder, value)
    }

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
