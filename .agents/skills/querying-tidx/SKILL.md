---
name: querying-tidx
description: "Query indexed blockchain data via the tidx.ts TypeScript client. Covers Tidx client creation, SQL queries for blocks/txs/logs/receipts, event CTE decoding with signatures, Kysely QueryBuilder, engine routing (PostgreSQL OLTP vs ClickHouse OLAP), live SSE streaming, and retry/error handling. Use when querying chain data with tidx.ts."
---

# Querying with tidx.ts

`tidx.ts` is a TypeScript client for querying indexed blockchain data via the tidx HTTP API. It provides two interfaces: a raw SQL `fetch`/`live` client (`Tidx`) and a type-safe Kysely-based `QueryBuilder`.

## Installation

```bash
npm install tidx.ts
pnpm add tidx.ts
bun add tidx.ts
```

## Usage

```ts
import { Tidx } from 'tidx.ts'

// Basic
const tidx = Tidx.create({ chainId: 42431 })

// With auth and custom URL
const tidx = Tidx.create({
  basicAuth: 'user:pass',
  baseUrl: 'https://tidx.tempo.xyz',
  chainId: 42431,
})
```

### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `basicAuth` | `string` | — | Basic auth credentials |
| `baseUrl` | `string` | `'https://tidx.tempo.xyz'` | API base URL |
| `chainId` | `number` | — | Chain ID (can also be set per-call) |

## Fetching Data (Raw SQL)

```ts
// Simple query
const result = await tidx.fetch({
  query: 'select hash, "from", "to", value from txs limit 10',
})
console.log(result.rows) // [{ hash: '0x...', from: '0x...', to: '0x...', value: '...' }, ...]

// With engine override
const result = await tidx.fetch({
  engine: 'clickhouse',
  query: 'select count(*) count from txs',
})

// With abort signal
const controller = new AbortController()
const result = await tidx.fetch({
  query: 'select * from blocks limit 5',
  signal: controller.signal,
})
```

### fetch Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `query` | `string` | — | SQL query (required) |
| `chainId` | `number` | client-level | Override chain ID |
| `engine` | `string` | auto | `'postgres'` or `'clickhouse'` |
| `signatures` | `Signature[]` | — | Event signatures for CTE tables |
| `retryCount` | `number` | `5` | Retry attempts on failure |
| + `RequestInit` | — | — | Standard fetch options (signal, headers, etc.) |

### Return Value

```ts
{
  columns: string[]
  rows: Record<string, unknown>[]
  row_count: number
  engine: string
  query_time_ms: number
}
```

## Event CTE Queries (Signatures)

Decode raw log data into typed columns using event signatures:

```ts
const result = await tidx.fetch({
  query: 'select "from", "to", value from transfer limit 10',
  signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
})
```

Multiple signatures:

```ts
const result = await tidx.fetch({
  query: 'select * from transfer limit 5',
  signatures: [
    'event Transfer(address indexed from, address indexed to, uint256 value)',
    'event Approval(address indexed owner, address indexed spender, uint256 value)',
  ],
})
```

## Live Streaming (SSE)

Subscribe to real-time updates via async generator:

```ts
const controller = new AbortController()

for await (const result of tidx.live({
  query: 'select hash, "from", "to", value from txs limit 10',
  signal: controller.signal,
})) {
  console.log(result.rows)
  // call controller.abort() to stop
}
```

With signatures:

```ts
for await (const result of tidx.live({
  query: 'select "from", "to", value from transfer limit 1',
  signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
  signal: controller.signal,
})) {
  console.log(result.rows)
}
```

## QueryBuilder (Kysely)

Type-safe query builder powered by [Kysely](https://kysely.dev):

```ts
import { Tidx, QueryBuilder } from 'tidx.ts'

const tidx = Tidx.create({ basicAuth: 'key', chainId: 42431 })
const qb = QueryBuilder.from(tidx)

// Query transactions
const txs = await qb
  .selectFrom('txs')
  .select(['hash', 'from', 'to', 'value'])
  .limit(10)
  .execute()
```

### With ABI signatures

```ts
const transfers = await qb
  .withSignatures(['event Transfer(address indexed from, address indexed to, uint256 value)'])
  .selectFrom('transfer')
  .select(['from', 'to', 'value'])
  .limit(100)
  .execute()
```

### With ABI object

```ts
const abi = [
  { type: 'event', name: 'Transfer', inputs: [...] }
] as const

const qb2 = qb.withAbi(abi)
const rows = await qb2
  .selectFrom('transfer')
  .select(['from', 'to', 'value'])
  .execute()
```

### Available tables

- `blocks` — block data (`num`, `hash`, `timestamp`, `gas_used`, etc.)
- `txs` — transactions (`hash`, `from`, `to`, `value`, `input`, etc.)
- `logs` — event logs (`address`, `selector`, `topic0`–`topic3`, `data`)
- `receipts` — receipts (`tx_hash`, `status`, `gas_used`, `contract_address`)

## Event Listeners

```ts
tidx.on('request', (request) => console.log('Request:', request.url))
tidx.on('response', (response) => console.log('Response:', response.status))
tidx.on('error', (error) => console.error('Error:', error.message))
tidx.on('*', (event, data) => console.log(event, data))

// Remove listeners
tidx.off('request')
tidx.off('*')
```

## Error Handling

Two error classes:

- `Tidx.FetchRequestError` — HTTP request failures (has `.status` and `.response`)
- `Tidx.SseError` — SSE stream errors (has `.type`: `'client'` or `'server'`)

```ts
try {
  await tidx.fetch({ query: 'invalid' })
} catch (error) {
  if (error instanceof Tidx.FetchRequestError) {
    console.error(error.message, error.status)
  }
}
```

### Retry behavior

- Retries automatically on 408, 429, 5xx errors and server SSE errors
- Exponential backoff: `min(200ms * 2^attempt, 30s)`
- Default 5 retries, configurable via `retryCount`
- Client SSE errors and 4xx (except 408/429) are NOT retried

## Query Examples

### Blocks

```ts
// Latest 5 blocks
const result = await tidx.fetch({
  query: 'select num, gas_used from blocks order by num desc limit 5',
})

// Block by number
const result = await tidx.fetch({
  query: 'select * from blocks where num = 1000000',
})

// Blocks in a time range
const result = await tidx.fetch({
  query: "select num, gas_used from blocks where timestamp > now() - interval '1 hour' order by num desc limit 100",
})
```

### Transactions

```ts
// Lookup by hash
const result = await tidx.fetch({
  query: "select * from txs where hash = '0x1234...'",
})

// Transactions from an address
const result = await tidx.fetch({
  query: `select hash, value, gas_used from txs where "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7' order by block_num desc limit 10`,
})

// Contract creations
const result = await tidx.fetch({
  query: 'select hash, "from" from txs where "to" is null order by block_num desc limit 10',
})
```

### Receipts

```ts
// Failed transactions
const result = await tidx.fetch({
  query: 'select tx_hash, "from", gas_used from receipts where status = 0 order by block_num desc limit 10',
})

// Contract deployments
const result = await tidx.fetch({
  query: 'select tx_hash, contract_address from receipts where contract_address is not null order by block_num desc limit 10',
})

// Transactions paid by a fee payer (Tempo-specific)
const result = await tidx.fetch({
  query: 'select tx_hash, "from", fee_payer from receipts where fee_payer is not null order by block_num desc limit 10',
})
```

### Logs

```ts
// Logs by contract address
const result = await tidx.fetch({
  query: "select block_num, tx_hash, data from logs where address = '0xABC...' order by block_num desc limit 10",
})

// Logs by event selector
const result = await tidx.fetch({
  query: "select * from logs where selector = '0xddf252ad' order by block_num desc limit 10",
})
```

### Event CTE Examples

```ts
// Transfer events with predicate pushdown on indexed params
const result = await tidx.fetch({
  query: `select "to", value from transfer where "from" = '0xdAC17F958D2ee523a2206206994597C13D831ec7' order by block_num desc limit 10`,
  signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
})

// Filter by contract address
const result = await tidx.fetch({
  query: `select "from", "to", value from transfer where address = '0xABC...' order by block_num desc limit 10`,
  signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
})
```

Filters on **indexed** params are automatically pushed down to topic-level WHERE clauses for index utilization.

`address` and `block_num` are **raw columns** always pushed down into the CTE.

Each CTE includes raw columns: `block_num`, `block_timestamp`, `log_idx`, `tx_idx`, `tx_hash`, `address`, `selector`, `topic1`–`topic3`, `data`, plus decoded columns from the signature.

### QueryBuilder Examples

```ts
import { Tidx, QueryBuilder } from 'tidx.ts'

const tidx = Tidx.create({ basicAuth: 'key', chainId: 42431 })
const qb = QueryBuilder.from(tidx)

// Latest blocks
const blocks = await qb
  .selectFrom('blocks')
  .select(['num', 'gas_used'])
  .orderBy('num', 'desc')
  .limit(5)
  .execute()

// Transfers with signatures
const transfers = await qb
  .withSignatures(['event Transfer(address indexed from, address indexed to, uint256 value)'])
  .selectFrom('transfer')
  .select(['from', 'to', 'value'])
  .orderBy('block_num', 'desc')
  .limit(10)
  .execute()

// OLAP aggregation
const qbOlap = QueryBuilder.from({ ...tidx, engine: 'clickhouse' })
const daily = await qbOlap
  .selectFrom('txs')
  .select(['block_timestamp', (eb) => eb.fn.count('hash').as('tx_count')])
  .groupBy('block_timestamp')
  .orderBy('block_timestamp', 'desc')
  .limit(30)
  .execute()
```

## Engine Routing: PostgreSQL vs ClickHouse

### PostgreSQL (OLTP) — default

Best for:
- **Point lookups** — tx by hash, block by number, address history
- **Real-time queries** — latest blocks, recent activity, live streaming
- **Low-latency** — sub-second responses for indexed lookups
- **Small result sets** — WHERE on indexed columns with LIMIT

```ts
// Point lookup (fast, uses hash index)
const result = await tidx.fetch({
  engine: 'postgres',
  query: "select * from txs where hash = '0x1234...'",
})

// Recent activity (fast, uses block_num DESC index)
const result = await tidx.fetch({
  engine: 'postgres',
  query: 'select * from blocks order by num desc limit 10',
})
```

### ClickHouse (OLAP) — `engine: 'clickhouse'`

Best for:
- **Aggregations** — COUNT, SUM, AVG over millions of rows
- **Full table scans** — analytics without narrow WHERE clauses
- **Time-series** — GROUP BY hour/day/week over large ranges
- **Heavy JOINs** — cross-table analytics

```ts
// Daily gas usage
const result = await tidx.fetch({
  engine: 'clickhouse',
  query: 'select toDate(block_timestamp) as day, sum(gas_used) as total_gas, count(*) as tx_count from txs group by day order by day desc limit 30',
})

// Transfer volume by token
const result = await tidx.fetch({
  engine: 'clickhouse',
  query: 'select address as token, count(*) as transfers, sum(value) as volume from transfer group by token order by transfers desc limit 20',
  signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
})
```

## Chain IDs

| Chain | ID |
|-------|-----|
| Tempo mainnet | `4217` |
| Tempo testnet | `42431` |
