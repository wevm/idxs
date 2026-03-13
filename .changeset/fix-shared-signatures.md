---
"idxs": patch
---

Fixed shared mutable signatures array in QueryBuilder

`QueryBuilder.from()` used a single shared `signatures` array across all `withSignatures()` calls. This caused signatures from separate query builder instances to accumulate and interfere with each other. For example, calling `qb.withSignatures([sigA])` followed by `qb.withSignatures([sigB])` would result in both instances seeing `[sigA, sigB]`, causing incorrect event table column resolution.

Each `inner()` call now creates an immutable snapshot of the signatures array, ensuring isolation between query builder instances.
