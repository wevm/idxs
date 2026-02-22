# tidx

TypeScript Interface for [tidx](https://tidx.tempo.xyz).

## Install

```bash
npm i tidx.ts
```

## Usage

### `Tidx`

Instantiate and use the `Tidx` client to fetch data from the tidx API.

```ts
import { Tidx } from 'tidx.ts'

const tidx = Tidx.create({ basicAuth: 'user:pass', chainId: 42431 })

// Fetch transactions
const txs = await tidx.fetch({
  query: 'select hash, "from", "to", value from txs limit 10',
})
console.log(txs.rows)

// Fetch Transfer events with ABI signature
const transfers = await tidx.fetch({
  query: 'select "from", "to", value from transfer limit 10',
  signatures: ['Transfer(address indexed from, address indexed to, uint256 value)'],
})
console.log(transfers.rows)

// Live streaming
for await (const result of tidx.live({
  query: 'select hash, "from", "to" from txs limit 10',
})) 
  console.log(result.rows)

```

### `QueryBuilder`

`tidx` exports a [Kysely-based](https://kysely.dev) type-safe query builder.

```ts
import { Tidx, QueryBuilder } from 'tidx.ts'

const tidx = Tidx.create({ basicAuth: 'your-api-key', chainId: 42431 })
const qb = QueryBuilder.from(tidx)

// Query standard tables
const txs = await qb
  .selectFrom('txs')
  .select(['hash', 'from', 'to', 'value'])
  .limit(10)
  .execute()

// Query with event signatures
const transfers = await qb
  .withSignatures(['event Transfer(address indexed from, address indexed to, uint256 value)'])
  .selectFrom('transfer')
  .select(['from', 'to', 'value'])
  .limit(100)
  .execute()

// Live streaming
for await (const txs of qb
  .selectFrom('txs')
  .select(['hash', 'from', 'to', 'value'])
  .stream()
)
  console.log(txs)
```

## API

TODO

## License

MIT
