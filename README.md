# idxs

TypeScript Interface for [Index Supply](https://indexsupply.net).

## Install

```bash
npm i idxs
```

## Usage

### `IndexSupply`

Instantiate and use the `IndexSupply` client to fetch data from the Index Supply API.

```ts
import { IndexSupply } from 'idxs'

const is = IndexSupply.create()

// Fetch transactions
const txs = await is.fetch({
  query: 'select hash, "from", "to", value from txs where chain = 1 limit 10',
})
console.log(txs.rows)

// Fetch Transfer events with ABI signature
const transfers = await is.fetch({
  query: 'select "from", "to", value from transfer where chain = 1 limit 10',
  signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
})
console.log(transfers.rows)

// Pagination with cursor
const next = await is.fetch({
  query: 'select hash, "from", "to", value from txs where chain = 1 limit 10',
  cursor: txs.cursor,
})

// Live streaming
for await (const result of is.live({
  query: 'select hash, "from", "to" from txs where chain = 1 limit 10',
})) 
  console.log(result.rows)

```

### `QueryBuilder`

`idxs` exports a [Kysely-based](https://kysely.dev) type-safe query builder.

```ts
import { IndexSupply, QueryBuilder } from 'idxs'

const is = IndexSupply.create()
const qb = QueryBuilder.from(is)

// Query standard tables
const txs = await qb
  .selectFrom('txs')
  .select(['hash', 'from', 'to', 'value'])
  .where('chain', '=', 1)
  .limit(10)
  .execute()

// Query with event signatures
const transfers = await qb
  .withSignatures(['event Transfer(address indexed from, address indexed to, uint256 value)'])
  .selectFrom('transfer')
  .select(['from', 'to', 'value'])
  .where('chain', '=', 1)
  .limit(100)
  .execute()

// Pagination with cursor
const next = await qb
  .atCursor(txs.cursor)
  .selectFrom('txs')
  .select(['hash'])
  .limit(10)
  .execute()
```

## API

TODO

## License

MIT