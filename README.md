# mq-bridge-meilisearch

A [Meilisearch](https://www.meilisearch.com/) endpoint for
[mq-bridge](https://github.com/marcomq/mq-bridge), talking to its REST API
directly. It works as an **output** (a document sink — what streaming database
changes into a search index needs) and as an **input** (a scan of an index's
documents).

It adds no Meilisearch dependency to mq-bridge itself: link it as a crate, or
load the compiled library as a plugin from any mq-bridge host, including the
Python and Node.js packages.

## Configuration

Register the endpoint before any route starts:

```rust
mq_bridge_meilisearch::register()?;
```

```yaml
output:
  custom:
    name: meilisearch
    config:
      url: "http://localhost:7700"
      api_key: "${MEILI_MASTER_KEY}"
      index: "movies"        # optional; route name by default
      primary_key: "id"
```

| Option | Applies to | Default | Meaning |
| --- | --- | --- | --- |
| `url` | both | *required* | Base URL. `meilisearch://` and `meilisearchs://` are rewritten to `http(s)://`. |
| `api_key` | both | none | Sent as `Authorization: Bearer`. Omit for an instance with no master key. |
| `index` | both | route name | Index UID. On an output, may contain templates. |
| `primary_key` | both | none | The field Meilisearch keys documents by. |
| `method` | output | `replace` | `replace` overwrites a document; `update` merges top-level fields into it. |
| `operation` | output | none | Each message's change operation, e.g. `${metadata:postgres.operation}`. |
| `delete_values` | output | `["delete"]` | Operation values that mean "remove this document". |
| `create_index` | output | `true` | Create the index before the first write. |
| `settings` | output | none | Index settings, forwarded verbatim to `PATCH /indexes/{uid}/settings`. |
| `wait_for_task` | output | `true` | Wait for the indexing task to finish before acknowledging. |
| `task_timeout_ms` | output | `60000` | How long to wait for one task. |
| `max_request_bytes` | output | `90000000` | Split a batch into several requests rather than exceed this body size. |
| `connect_timeout_ms` | both | `10000` | TCP/TLS connect timeout. |
| `request_timeout_ms` | both | none | Whole-request timeout. |
| `fields` | input | all | Comma-separated document fields to read. |
| `cursor_id` | input | none | Names this reader's position; without it every restart re-reads from the start. |
| `checkpoint_store` | input | none | Where that position is persisted: a `file://` spec or a plain path. |
| `polling_interval_ms` | input | `1000` | Delay between polls once the scan has reached the end. |
| `max_polling_interval_ms` | input | none | Upper bound the idle delay backs off to. |

Set `primary_key` whenever you can. Left unset, Meilisearch infers it from the
first batch by picking a field whose name contains `id` — possibly the wrong
one, permanently.

Values written `${metadata:<key>}` and `${payload:<field>}` are resolved per
message, and may sit inside surrounding text (`app_${metadata:postgres.table}`).
A message that leaves `index` unresolved is dead-lettered rather than written
somewhere arbitrary. An unresolved `operation` counts as an upsert, which is
what a backfilled row, carrying no operation, is.

## Streaming Postgres changes into an index

Messages are already JSON objects, so a table needs no mapping:

```yaml
routes:
  movies_to_search:
    batch_size: 1000
    input:
      postgres_cdc:
        url: "postgres://user:pass@localhost/app"
        publication: "movies_pub"
    output:
      custom:
        name: meilisearch
        config:
          url: "http://localhost:7700"
          api_key: "${MEILI_MASTER_KEY}"
          index: "${metadata:postgres.table}"     # or a fixed name
          primary_key: "id"
          operation: "${metadata:postgres.operation}"
```

`operation` classifies each message as an upsert or a delete: `delete_values`
decides which values remove a document, everything else is an upsert. A
`truncate` carries no row, so it is dead-lettered rather than indexed as an
empty document. Without `operation`, every message is an upsert — all a one-off
copy or a file import needs.

A templated `index` routes each message to the index its own metadata names, so
one route can carry every table in a publication. Those indexes are created, and
their `settings` applied, on first write. Templating is sink-only; an input
pages through one index.

### Index settings

`settings` is handed to `PATCH /indexes/{uid}/settings` before the first
document, so a route can stand up a usable index from nothing:

```yaml
settings:
  searchableAttributes: ["title", "overview"]
  filterableAttributes: ["genre", "year"]
```

The body is passed through untouched, so anything Meilisearch accepts works,
including settings added after this release. `PATCH` merges, so keys you leave
out keep their current value, and a key Meilisearch rejects fails the route at
startup instead of being dropped silently.

### Delivery

A batch costs one request per operation and index: all its upserts to an index
go out together, and so do its deletes. Only writes to the same document keep
their order — reordering `insert(id=7)` and `delete(id=7)` would leave document
7 in the index for good — so a document switching operation starts a new
segment, and segments are issued in order. Without `primary_key` documents
cannot be told apart, and every switch starts one. A failing request fails its
messages and every later segment, so the route never sees an unconfirmed write
reported as delivered. A request over `max_request_bytes` is split into several
ordered ones rather than rejected as `payload_too_large`.

Meilisearch fails a whole request for one bad document, so documents it would
refuse — no `primary_key` field, an invalid id, a payload that is not a JSON
object — are rejected on their own before sending.

Meilisearch answers a write with `202 Accepted` and applies it afterwards, so
`wait_for_task` polls the task to a finished state before acknowledging.
Otherwise the source's replication slot advances past writes that can still fail
(an invalid `_geo`, for one), losing rows silently. That is one round trip per
batch — negligible at `batch_size: 1000`.

### Ordering and `concurrency`

Two batches touching one document must arrive in source order, or an older row
overwrites a newer one silently. The endpoint declares `requires_ordered_publish`,
so the route serialises its sends **at any `concurrency`** — linked as a crate,
or loaded as a plugin by mq-bridge 0.4.13 or newer (plugin ABI 1.1).

ABI 1.1 also reports a failure per message, so a plugin-loaded sink behaves like
a linked one: only the failed messages, and those behind them, are retried or
dead-lettered, and the writes already confirmed are not sent again.

## Backfilling an existing table

CDC carries changes, not the rows already there. mq-bridge's `postgres_cdc`
source reads those too with `consume: capture_all` (mq-bridge 0.4.13+): it
creates the replication slot first, pages each table in the publication by its
primary key, then streams from the slot — one route, no gap.

```yaml
routes:
  movies_to_search:
    batch_size: 1000
    input:
      postgres_cdc:
        url: "postgres://user:pass@localhost/app"
        publication: "movies_pub"
        slot_name: "mqb_meili"
        consume: capture_all
        cursor_id: "movies_backfill"
        checkpoint_store: "file:///var/lib/mq-bridge/movies-phase.json"
    output:
      custom:
        name: meilisearch
        config:
          url: "http://localhost:7700"
          api_key: "${MEILI_MASTER_KEY}"
          index: "movies"
          primary_key: "id"
          operation: "${metadata:postgres.operation}"
```

- **Use a literal `index`.** Backfilled rows are read by a table scan and carry
  neither `postgres.operation` nor `postgres.table`. The missing operation makes
  them upserts, as intended, but `${metadata:postgres.table}` cannot resolve and
  would dead-letter them. Give each table its own publication and route.
- **Delivery is at-least-once across the handover.** A row changed during the
  scan is read twice, once by the scan and once from the slot. Both are keyed by
  `primary_key`, so the replay corrects anything the scan wrote stale.
- **`cursor_id` + `checkpoint_store` make it resumable.** A restart skips tables
  already scanned and continues the current one from its last key; without them
  every restart scans again from the start. Write the store as `file:///…`:
  mq-bridge reads a plain path as a table name in the source database.
- **The primary key must be a single column.** Otherwise spell the phases out
  as a `sequence`, below.
- Watch the slot's lag while the scan runs: an unread slot retains WAL.

### Spelling the phases out: `sequence`

`capture_all` is shorthand for mq-bridge's `sequence` input, which runs its
inputs one after another in a single route: each is drained before the next
starts, and the last one streams. Write it by hand when the shorthand does not
fit — a composite or non-integer key (page by another unique, increasing
column), only some of the publication's tables, or a backfill from somewhere
else entirely, such as a JSONL export:

```yaml
input:
  sequence:
    endpoints:
      - sqlx:
          url: "postgres://user:pass@localhost/app"
          table: "public.movies"
          cursor_column: "movie_seq"
          cursor_id: "movies_scan"
          checkpoint_store: "file:///var/lib/mq-bridge/movies-scan.json"
      - postgres_cdc:
          url: "postgres://user:pass@localhost/app"
          publication: "movies_pub"
          slot_name: "mqb_meili"
    cursor_id: "movies_backfill"
    checkpoint_store: "file:///var/lib/mq-bridge/movies-phase.json"
```

The output is the same as above. Before the first phase reads anything, the
`postgres_cdc` phase creates its slot, so the handover is gapless here too. Each
phase is a full endpoint and may carry its own `middlewares`, e.g. a `transform`
that reshapes the export into the table's shape. Everything above about a
literal `index` and at-least-once delivery applies unchanged. See mq-bridge's
[REFERENCE.md](https://github.com/marcomq/mq-bridge/blob/main/docs/REFERENCE.md#sequence).

### On Supabase

Same as any Postgres, with three things that otherwise look like bugs:

- **Use the direct connection** (`db.<ref>.supabase.co:5432`), not the pooler —
  a pooled connection cannot start replication.
- **It is IPv6-only** unless you have the IPv4 add-on. Without an IPv6 route the
  connection just times out; check with `ping6` first.
- **Add your own publication.** Do not stream from `supabase_realtime`.
  `wal_level` is already `logical`.

## Merging rows from different tables

`method: update` merges top-level fields into an existing document, so one route
per source table — all writing the same `index` and `primary_key` — makes
Meilisearch perform the join. It joins on **the document's primary key only**:
each message updates exactly the one document its own key names.

**Tables sharing a key** (one-to-one) merge directly. `movie_stats` keyed by
`movie_id` becomes part of the `movies` document once a `transform` renames the
key and picks the fields to merge:

```yaml
routes:
  movie_stats_to_search:
    input:
      postgres_cdc:
        url: "postgres://user:pass@localhost/app"
        publication: "movie_stats_pub"
    output:
      middlewares:
        - transform:
            mapping:
              id: "$.movie_id"
              rating: "$.rating"
              votes: "$.votes"
      custom:
        name: meilisearch
        config:
          url: "http://localhost:7700"
          index: "movies"
          primary_key: "id"
          method: update
```

**A foreign key does not merge.** A `directors` row changes the documents of
every movie pointing at it, but the sink writes one document per message and
cannot look up which ones those are. Denormalise in Postgres instead — keep
`director_name` on `movies`, or maintain a table the publication covers with a
trigger — so each row already is the document you want indexed. The same goes
for one-to-many children such as a movie's genres: a list replaces the whole
field (see [Limitations](#limitations)), so build it upstream.

**Deletes remove the whole document.** With `operation` mapped, deleting a
`movie_stats` row deletes the movie, not just its rating. Map `operation` only
on the route that owns the document (`movies`) and leave it unset on the others:
there a delete becomes an update carrying only the key (under the default
replica identity), which changes nothing — the old rating stays until the next
write. Under `REPLICA IDENTITY FULL` the delete carries the whole old row, so it
writes the old values back instead. Routes are not ordered against each other, and `update` creates a
document that does not exist yet, so a `movie_stats` change landing after its
movie was deleted leaves a stub document holding only those fields.

## Reading an index back out

As an input, the endpoint pages through `GET /indexes/{uid}/documents` and emits
each document as one message, with `meilisearch.index` and (with `primary_key`)
`meilisearch.document_id` metadata.

```yaml
input:
  custom:
    name: meilisearch
    config:
      url: "http://localhost:7700"
      index: "movies"
      cursor_id: "export"
      checkpoint_store: "file:///var/lib/mq-bridge/meilisearch.json"
```

`/documents` rather than `/search`, because search pagination stops at
`maxTotalHits` (1000 by default) and cannot export a whole index.

Paging is by offset, so it is stable only for an index nobody is writing to —
for an export, drain a quiet index. A nacked batch rolls back to the last
acknowledged document; a batch dropped without committing does not, as with
mq-bridge's other cursor-paged readers.

## Use it from any mq-bridge process

The crate also builds a `cdylib`, so a host that never compiled against it can
load it at runtime:

```console
mqb copy --plugin ./libmq_bridge_meilisearch.so 'file://movies.jsonl?format=raw' \
  'meilisearch://localhost:7700?index=movies&primary_key=id&api_key=KEY'
```

```rust
mq_bridge::plugin::load_endpoint_plugin("./libmq_bridge_meilisearch.so")?;
```

The library speaks plugin ABI 1.1, so the host must be mq-bridge 0.4.13 or
newer. A URI query carries no types, so the host maps one against the schema
this endpoint declares: `create_index=false`, `task_timeout_ms=30000` and
`delete_values=delete,remove` reach the endpoint as a boolean, a number and a
list.

A package manager puts the same library where mq-bridge already looks, so
nothing has to name a path:

```console
brew install marcomq/tap/mq-bridge-meilisearch
conda install -c marcomq mq-bridge-meilisearch
```

Python and Node.js users install a package that ships this library and hands its
path to mq-bridge's generic loader; the configuration is identical in every
language.

```console
pip install mq-bridge mq-bridge-meilisearch      # then: mq_bridge_meilisearch.register()
npm install mq-bridge mq-bridge-meilisearch      # then: import { register } from ...
```

See [PLUGINS.md](https://github.com/marcomq/mq-bridge/blob/main/docs/PLUGINS.md)
for how loading, versioning and the ABI work, and
[CONTRIBUTING.md](CONTRIBUTING.md) for building and packaging a release.

## Limitations

Version 0.1 passes its suite against a real Meilisearch but has no production
mileage — run it beside whatever you have now before cutting over.

- **Joins are by primary key only.** A foreign-key join has to be denormalised
  upstream; see [Merging rows](#merging-rows-from-different-tables). If your
  documents are built from lookups across tables, a CDC tool with SQL
  enrichment (such as Sequin) fits better than this sink.
- **`update` merges top-level fields only**: `{"tags": ["a"]}` replaces the whole
  array. Meilisearch's function-based edit applies to a filtered document set
  rather than one document per message, so it does not fit this sink. Compute
  the value upstream.
- **No replay log.** Recovering from a bad transform means re-running the
  backfill. Put a durable queue in front of the sink if you need better.
- **`checkpoint_store` takes a file only.** Write it as `file:///…`, the form
  `postgres_cdc` needs too. mq-bridge's SQL, Mongo and
  object-store backends sit behind its own feature flags, which a plugin
  `cdylib` links with off.
- **Per-message observability is thin.** A failed write reaches the route's
  dead-letter queue carrying Meilisearch's own error code; that is where to
  look first.

## Tests

```console
cargo test --lib                                     # no dependencies
cargo test --test integration -- --ignored           # starts a container
cargo test --test plugin -- --ignored                # same suite, direct vs plugin-loaded
```

Both Docker-backed files start Meilisearch from `tests/docker-compose.yml`,
which is why they are ignored by default. There is a runnable example in
`examples/`, and [CONTRIBUTING.md](CONTRIBUTING.md) covers what the suites
cover, packaging and releases.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
