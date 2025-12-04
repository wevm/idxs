import type { AbiParameterToPrimitiveType, AbiType, ParseAbiItem } from 'abitype'
import * as AbiItem from 'ox/AbiItem'
import type * as Hex from 'ox/Hex'
import * as z from 'zod/mini'
import type {
  CaseInsensitive,
  Compute,
  IsNarrowable,
  LastWord,
  Trim,
  Unquote,
  Whitespace,
} from './types.js'

/** Raw result from Index Supply. */
export type Raw = {
  columns: readonly { name: string; pgtype: string }[]
  cursor: string
  rows: unknown[][]
}

/** Result. */
export type Result<
  sql extends string = string,
  signatures extends readonly Signature[] | undefined = undefined,
> = {
  cursor: string
  rows: ToRows<sql, signatures>
}

/** Stringified signature of a function or event. */
export type Signature = `function ${string}` | `event ${string}`

/** Type map of standard column types. */
export type StandardColumnTypes = {
  [K in keyof typeof standardColumnTypes]: z.infer<(typeof standardColumnTypes)[K]>
}

/** Parses the SQL query and returns a type mapping of the rows. */
export type ToRows<
  sql extends string = string,
  signatures extends readonly Signature[] | undefined = undefined,
> = IsNarrowable<sql, string> extends true
  ? ExtractTableName<sql> extends infer tableName extends string
    ? IsStandardTable<tableName> extends true
      ? ProcessColumns<sql, signatures>
      : signatures extends readonly Signature[]
        ? signatures['length'] extends 0
          ? SignatureRequiredError<tableName>
          : ProcessColumns<sql, signatures>
        : SignatureRequiredError<tableName>
    : never
  : Record<string, unknown>[]

export const standardColumnTypes = {
  address: z.templateLiteral(['0x', z.string()]),
  block_num: z.transform((value: number) => BigInt(value)),
  block_timestamp: z.transform((value: string) => {
    const [pgDate, pgTime] = value.split(' ', 2)
    if (!pgTime) throw new Error('Invalid timestamp format (missing time)')

    const [time] = pgTime.split('.')
    if (!time) throw new Error('Invalid timestamp format (invalid time)')

    const [h, m, s] = time.split(':')
    if (!h || !m || !s) throw new Error('Invalid timestamp format (invalid time)')

    const parsed = Date.parse(`${pgDate}T${h.padStart(2, '0')}:${m}:${s}Z`)
    if (Number.isNaN(parsed)) throw new Error('Invalid timestamp format (could not parse)')

    return Math.floor(parsed / 1000)
  }),
  chain: z.number(),
  data: z.templateLiteral(['0x', z.string()]),
  extra_data: z.templateLiteral(['0x', z.string()]),
  from: z.templateLiteral(['0x', z.string()]),
  gas: z.transform((value: string) => BigInt(value)),
  gas_limit: z.bigint(),
  gas_price: z.transform((value: string) => BigInt(value)),
  gas_used: z.bigint(),
  hash: z.templateLiteral(['0x', z.string()]),
  idx: z.number(),
  input: z.templateLiteral(['0x', z.string()]),
  log_idx: z.number(),
  miner: z.templateLiteral(['0x', z.string()]),
  nonce: z.transform((value: string) => BigInt(value)),
  num: z.bigint(),
  receipts_root: z.templateLiteral(['0x', z.string()]),
  size: z.number(),
  state_root: z.templateLiteral(['0x', z.string()]),
  timestamp: z.number(),
  to: z.templateLiteral(['0x', z.string()]),
  topics: z.array(z.string()),
  tx_hash: z.templateLiteral(['0x', z.string()]),
  type: z.number(),
  value: z.transform((value: string) => BigInt(value)),
}

const standardTables = ['txs', 'logs', 'blocks']

/** Extracts the table name from the SQL query. */
function extractTableName(query: string): string | null {
  const match = query.toLowerCase().match(/\bfrom\s+(\w+)/i)
  if (!match || !match[1]) return null
  return match[1]
}

/** Extracts the table name from the column name. */
function getColumnTableName({ name, query }: { name: string; query: string }): string | null {
  // Match patterns like: txs.column, "txs".column, txs."column", "txs"."column"
  const pattern = new RegExp(`"?(\\w+)"?\\."?${name}"?(?:\\s|,|$)`, 'i')
  const match = query.match(pattern)
  if (match?.[1]) return match[1]
  return null
}

/** Parses the raw result into a structured result. */
export function parse<
  const sql extends string = string,
  const signatures extends readonly Signature[] | undefined = undefined,
>(raw: Raw, options: parse.Options<sql, signatures>): Result<sql, signatures> {
  const { query, signatures } = options

  const rows: Record<string, unknown>[] = []
  const sourceTableName = extractTableName(query)

  const signatureTypes: Record<string, Record<string, z.ZodMiniType>> = {}
  for (const signature of signatures ?? []) {
    const abiItem = AbiItem.from(signature)
    if (!('name' in abiItem)) continue

    const types: Record<string, z.ZodMiniType> = {}
    for (const input of abiItem.inputs) {
      if (!input.name) continue

      const transform = (() => {
        const type = input.type as AbiType
        if (type === 'address') return z.templateLiteral(['0x', z.string()])
        if (type === 'bool') return z.boolean()
        if (type.startsWith('bytes')) {
          if (type.includes('[')) return z.array(z.templateLiteral(['0x', z.string()]))
          return z.templateLiteral(['0x', z.string()])
        }
        if (type.startsWith('int') || type.startsWith('uint')) {
          const size = type.match(/int(\d+)$/)?.[1]
          const inner = (() => {
            if (size && Number(size) <= 48) return z.transform((value: string) => Number(value))
            return z.transform((value: string) => BigInt(value))
          })()
          if (type.includes('[')) return z.array(inner)
          return inner
        }
        return z.string()
      })()

      types[input.name] = transform
    }

    signatureTypes[abiItem.name.toLowerCase()] = types
  }

  for (let i = 0; i < raw.rows.length; i++) {
    const row_raw = raw.rows[i]
    const row: Record<string, unknown> = {}
    for (let j = 0; j < raw.columns.length; j++) {
      const column = raw.columns[j]
      if (!row_raw) continue
      if (!column) continue

      const { name } = column
      const tableName = getColumnTableName({ name, query }) ?? sourceTableName

      const value = (() => {
        const value = row_raw[j]

        if (!tableName) return value

        const type = (() => {
          if (standardTables?.includes(tableName))
            return standardColumnTypes[name as keyof typeof standardColumnTypes]
          return (
            signatureTypes[tableName]?.[name] ??
            standardColumnTypes[name as keyof typeof standardColumnTypes]
          )
        })()
        if (!type) return value

        return type.parse(value)
      })()

      row[name] = value
    }
    rows.push(row)
  }

  return {
    cursor: raw.cursor,
    rows,
  }
}

export declare namespace parse {
  export type Options<
    sql extends string = string,
    signatures extends readonly Signature[] | undefined = undefined,
  > = {
    query: sql | string
    signatures?: signatures | readonly string[] | undefined
  }
}

/**
 * Strip type cast (e.g., `::date`, `::timestamp`) from column name.
 */
type StripTypeCast<value extends string> = value extends `${infer column}::${infer _}`
  ? Trim<column>
  : value

/**
 * Split columns by comma, respecting nested parentheses and quotes.
 * Uses tuple length to track parenthesis depth for proper nesting support.
 */
type SplitColumns<
  columns extends string,
  acc extends string[] = [],
  current extends string = '',
  // biome-ignore lint/suspicious/noExplicitAny: _
  depth extends any[] = [],
  quoteType extends false | '"' | "'" = false,
> = columns extends `${infer x}${infer Rest}`
  ? // Handle quotes - only toggle if matching quote type or not in quote
    x extends '"'
    ? quoteType extends false
      ? SplitColumns<Rest, acc, `${current}${x}`, depth, '"'>
      : quoteType extends '"'
        ? SplitColumns<Rest, acc, `${current}${x}`, depth, false>
        : SplitColumns<Rest, acc, `${current}${x}`, depth, quoteType>
    : x extends "'"
      ? quoteType extends false
        ? SplitColumns<Rest, acc, `${current}${x}`, depth, "'">
        : quoteType extends "'"
          ? SplitColumns<Rest, acc, `${current}${x}`, depth, false>
          : SplitColumns<Rest, acc, `${current}${x}`, depth, quoteType>
      : // Handle comma (only split if not in quotes and depth is 0)
        x extends ','
        ? quoteType extends false
          ? depth extends []
            ? SplitColumns<Rest, [...acc, current], '', [], false>
            : SplitColumns<Rest, acc, `${current}${x}`, depth, false>
          : SplitColumns<Rest, acc, `${current}${x}`, depth, quoteType>
        : // Handle parentheses (only count if not in quotes)
          x extends '('
          ? quoteType extends false
            ? SplitColumns<Rest, acc, `${current}${x}`, [...depth, 1], false>
            : SplitColumns<Rest, acc, `${current}${x}`, depth, quoteType>
          : x extends ')'
            ? quoteType extends false
              ? // biome-ignore lint/suspicious/noExplicitAny: _
                depth extends [...infer restDepth extends any[], any]
                ? SplitColumns<Rest, acc, `${current}${x}`, restDepth, false>
                : SplitColumns<Rest, acc, `${current}${x}`, [], false>
              : SplitColumns<Rest, acc, `${current}${x}`, depth, quoteType>
            : SplitColumns<Rest, acc, `${current}${x}`, depth, quoteType>
  : current extends ''
    ? acc
    : [...acc, current]

/**
 * Check if the last word looks like a simple alias (not part of expression).
 */
type HasSpaceSeparatedAlias<value extends string> = LastWord<value> extends infer Last extends
  string
  ? Last extends value
    ? false // No whitespace found, Last is the entire string
    : // Check if Last looks like a simple alias (no special chars)
      Last extends `${infer _}::${infer __}` // Has type cast
      ? false
      : Last extends `${infer _}(${infer __}` // Has parentheses
        ? false
        : Last extends `${infer _}"` // Ends with quote - probably part of identifier
          ? false
          : Last extends `${infer _})` // Ends with paren - probably part of expression
            ? false
            : true
  : false

/**
 * Strip table prefix (e.g., "t1.column" -> "column", "t1."quoted"" -> ""quoted"")
 */
type StripTablePrefix<T extends string> = T extends `${infer _}.${infer column}` ? column : T

/**
 * Extract alias from column expression
 */
type ExtractColumnName<value extends string> =
  // Handle "expr as alias" or "expr as "alias""
  value extends `${infer _} as "${infer Alias}"`
    ? Unquote<Alias>
    : value extends `${infer _} as ${infer Alias}`
      ? Unquote<Trim<Alias>>
      : // Handle space-separated alias (e.g., "max(block_num) block" or "address token")
        HasSpaceSeparatedAlias<Trim<value>> extends true
        ? Unquote<Trim<LastWord<Trim<value>>>>
        : // No alias, use the column name (strip table prefix, type cast, and unquote)
          Unquote<StripTypeCast<StripTablePrefix<Trim<value>>>>

/**
 * Builds a map of column names to ABI types from event inputs
 */
type BuildAbiTypeMap<
  inputs extends readonly { name: string; type: string }[],
  // biome-ignore lint/complexity/noBannedTypes: _
  acc extends Record<string, string> = {},
> = inputs extends readonly [
  infer head extends { name: string; type: string },
  ...infer tail extends readonly { name: string; type: string }[],
]
  ? BuildAbiTypeMap<tail, acc & { [K in head['name']]: head['type'] }>
  : acc

/**
 * Builds a type map from multiple event signatures
 */
type BuildTypeMapFromSignatures<
  signatures extends readonly string[],
  // biome-ignore lint/complexity/noBannedTypes: _
  acc extends Record<string, string> = {},
> = signatures extends readonly [infer head extends string, ...infer tail extends readonly string[]]
  ? ParseAbiItem<head> extends infer Event
    ? Event extends { inputs: infer Inputs }
      ? Inputs extends readonly { name: string; type: string }[]
        ? BuildTypeMapFromSignatures<tail, acc & BuildAbiTypeMap<Inputs>>
        : BuildTypeMapFromSignatures<tail, acc>
      : BuildTypeMapFromSignatures<tail, acc>
    : BuildTypeMapFromSignatures<tail, acc>
  : acc

/**
 * Infer type based on Index Supply column types.
 */
type InferColumnType<
  columnName extends string,
  signature extends readonly string[] | string | undefined = undefined,
> = signature extends readonly string[]
  ? BuildTypeMapFromSignatures<signature> extends infer TypeMap
    ? columnName extends keyof TypeMap
      ? TypeMap[columnName] extends AbiType
        ? AbiParameterToPrimitiveType<{ type: TypeMap[columnName] }>
        : string
      : // Fallback to database schema types
        columnName extends 'topics'
        ? string[]
        : columnName extends keyof StandardColumnTypes
          ? StandardColumnTypes[columnName]
          : string
    : string
  : signature extends string
    ? ParseAbiItem<signature> extends infer Event
      ? Event extends { inputs: infer Inputs }
        ? Inputs extends readonly { name: string; type: string }[]
          ? BuildAbiTypeMap<Inputs> extends infer TypeMap
            ? columnName extends keyof TypeMap
              ? TypeMap[columnName] extends AbiType
                ? AbiParameterToPrimitiveType<{ type: TypeMap[columnName] }>
                : string
              : // Fallback to database schema types
                columnName extends 'topics'
                ? string[]
                : columnName extends keyof StandardColumnTypes
                  ? StandardColumnTypes[columnName]
                  : string
            : string
          : string
        : string
      : string
    : // Array types (bytea[]) from database schema
      columnName extends 'topics'
      ? Hex.Hex[]
      : columnName extends keyof StandardColumnTypes
        ? StandardColumnTypes[columnName]
        : string

/**
 * Parses a single column expression into { name, type }
 */
type ParseColumn<
  column extends string,
  signature extends readonly string[] | string | undefined = undefined,
> = {
  [k in ExtractColumnName<column>]: InferColumnType<k, signature>
}

/**
 * Merges column objects into a single type
 */
type MergeColumns<
  columns extends string[],
  signature extends readonly string[] | string | undefined = undefined,
> = columns extends [infer head extends string, ...infer tail extends string[]]
  ? ParseColumn<head, signature> & MergeColumns<tail, signature>
  : // biome-ignore lint/complexity/noBannedTypes: _
    {}

/**
 * Extracts the columns from the SQL query
 */
type ExtractColumns<sql extends string> =
  sql extends `${infer _}${CaseInsensitive<'select'>}${infer rest}`
    ? Trim<rest> extends infer afterSelect extends string
      ? afterSelect extends `${infer columns}${Whitespace}${CaseInsensitive<'from'>}${infer _}`
        ? Trim<columns>
        : Trim<afterSelect>
      : never
    : never

/**
 * Extracts the table name from the FROM clause
 */
type ExtractTableName<sql extends string> =
  sql extends `${infer _}${CaseInsensitive<'from'>}${Whitespace}${infer rest}`
    ? Trim<rest> extends infer afterFrom extends string
      ? afterFrom extends `${infer table} ${infer _}`
        ? Trim<table>
        : afterFrom extends `${infer table}\n${infer _}`
          ? Trim<table>
          : afterFrom extends `${infer table}\t${infer _}`
            ? Trim<table>
            : Trim<afterFrom>
      : never
    : never

/**
 * Checks if the table is a standard Index Supply table
 */
type IsStandardTable<table extends string> = table extends 'blocks' | 'txs' | 'logs' ? true : false

/**
 * Processes the columns into a row type
 */
type ProcessColumns<
  sql extends string,
  signature extends readonly string[] | string | undefined,
> = ExtractColumns<sql> extends infer columns extends string
  ? SplitColumns<columns> extends infer columns extends string[]
    ? // biome-ignore lint/suspicious/noExplicitAny: _
      MergeColumns<columns, signature> extends infer Row extends Record<string, any>
      ? readonly Compute<Row>[]
      : never
    : never
  : never

/**
 * Signature validation error
 */
type SignatureRequiredError<tableName extends string> = {
  error: 'A signature is required for non-standard tables. Please provide an event signature as the second parameter.'
  table: tableName
}
