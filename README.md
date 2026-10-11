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
| `method` | output | `replace` | `replace` overwrites a document; `update` merges top-level fields into it; `update_existing` merges only into a document that already exists. |
| `update_where` | output | none | `<attribute> = <value>`, e.g. `author.id = ${payload:id}`: write each message into the documents it matches instead of indexing it. See [Merging rows](#merging-rows-from-different-tables). |
| `update_into` | output | none | The top-level field `update_where` writes the message to. |
| `operation` | output | none | Each message's change operation, e.g. `${metadata:postgres.operation}`. |
| `delete_values` | output | `["delete"]` | Operation values that mean "remove this document". |
| `create_index` | output | `true` | Create the index before the first write. |
| `swap_into` | output | none | The live index a rebuild replaces: once the input is drained, `index` is swapped with it and deleted. See [Rebuilding an index](#rebuilding-an-index). |
| `settings` | output | none | Index settings, forwarded verbatim to `PATCH /indexes/{uid}/settings`. |
| `wait_for_task` | output | `true` | Wait for the indexing task to finish before acknowledging. |
| `task_timeout_ms` | output | `60000` | How long one task may take before a warning is logged. The wait continues; a slow task is never resent. |
| `max_request_bytes` | output | `90000000` | Split a batch into several requests rather than exceed this body size. |
| `compression` | output | `none` | `gzip` compresses document request bodies (`Content-Encoding: gzip`). `max_request_bytes` still counts uncompressed bytes, which is what Meilisearch's payload limit applies to. |
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
startup instead of being dropped silently. The route starts only once that
settings task has finished, so on an index that already holds documents a
changed setting delays the start by the reindex it causes.

Declare `searchableAttributes`, `filterableAttributes` and `embedders` here
**before the first load**. Meilisearch indexes every field as searchable until
told otherwise, and changing any of the three later reindexes every document
already stored.

Vectors computed upstream go in `_vectors`, under an embedder declared as
`userProvided`:

```yaml
output:
  middlewares:
    - transform:
        mapping:
          id: "$.id"
          title: "$.title"
          "_vectors.default": "$.embedding"
        schema:
          type: object
          properties:
            _vectors:
              type: object
              properties:
                default:
                  type: ["string", "null"]
                  contentMediaType: application/json
  custom:
    name: meilisearch
    config:
      url: "http://localhost:7700"
      index: "movies"
      primary_key: "id"
      settings:
        embedders:
          default: { source: userProvided, dimensions: 384 }
```

The `schema` is only needed when the vector arrives as a JSON string, as a
`jsonb` or array column does from a backfill: `contentMediaType` parses it into
the array Meilisearch expects, and `null` is allowed so that a delete, which
carries the key and nothing else, is not rejected by the transform.

### Delivery

A batch costs one request per operation and index: all its upserts to an index
go out together, and so do its deletes. Only writes to the same document keep
their order — reordering `insert(id=7)` and `delete(id=7)` would leave document
7 in the index for good — so a document switching operation starts a new
segment, and segments are issued in order. Without `primary_key` documents
cannot be told apart, and every switch starts one. A failing request fails its
messages and every later segment, so the route never sees an unconfirmed write
reported as delivered. A request over `max_request_bytes` is split into several
ordered ones rather than rejected as `payload_too_large`. Meilisearch itself
recommends 50,000 to 100,000 documents per request for a bulk load, so raise
`batch_size` for a backfill rather than sending many small batches.

`mqb copy` (mq-bridge 0.4.18+) does that by itself: with no `--batch-size` it
uses the 50,000 this endpoint asks for. A route in a config file keeps its own
`batch_size`. The batch is held in memory, so size it to the documents: 50,000
small rows took 200 to 280 MB in our runs, while a row carrying a
1,024-dimension vector is about 46 KB, over 2 GB per batch. Pass a smaller
`--batch-size` for documents like that.

Meilisearch fails a whole request for one bad document, so documents it would
refuse — no `primary_key` field, an invalid id, a payload that is not a JSON
object — are rejected on their own before sending.

Meilisearch answers a write with `202 Accepted` and applies it afterwards, so
`wait_for_task` polls the task to a finished state before acknowledging.
Otherwise the source's replication slot advances past writes that can still fail
(an invalid `_geo`, for one), losing rows silently. That is one round trip per
batch — negligible at `batch_size: 1000`.

The wait has no upper bound. Meilisearch does not lose an enqueued task, so a
batch whose task is merely slow is never handed back to `retry`: resending it
would index the same documents a second time behind the first. Each
`task_timeout_ms` that passes logs a warning instead, and a poll that fails
(Meilisearch restarting, a `503`) is repeated. The batch fails only when the
task itself finishes as `failed` or `canceled`.

One resend remains possible: a documents request that times out before its
`202` arrives — `request_timeout_ms`, a proxy, or a `timeout` middleware around
the sink — may already be enqueued, and the retry enqueues it again. With
`primary_key` set that costs indexing time but changes nothing: both writes
carry the same documents under the same keys.

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

**A foreign key is joined with `lookup`** (mq-bridge 0.4.16+). A `lookup`
middleware in front of the sink reads the referenced row and writes it into the
document before it is indexed; with the key inside `IN (…)` it costs one query
per batch, not per message:

```yaml
output:
  middlewares:
    - lookup:
        from:
          sqlx:
            url: "postgres://user:pass@localhost/app"
            table: "authors"
            lookup_query: "SELECT id, name FROM authors WHERE id IN (${payload:author_id}::int)"
        into: author
  custom:
    name: meilisearch
    config: { url: "http://localhost:7700", index: "books", primary_key: "id" }
```

[`examples/postgres_to_meilisearch.yaml`](examples/postgres_to_meilisearch.yaml)
puts it together: `books` owns the document and looks up its author and its
stats, and a second route follows later `book_stats` changes with
`update_existing`. Two things `lookup` does not do:

- **A parent change does not reach its children.** Renaming an author leaves
  existing books with the old name; a second route with `update_where`, below,
  pushes the change into them.
- **It answers with one row.** A one-to-many child, such as a book's genres,
  still has to be built upstream; a list replaces the whole field (see
  [Limitations](#limitations)).

**`update_where` pushes a parent change into its children** (0.1.3+). A route on the
parent table writes each changed row into every document that embeds it:

```yaml
routes:
  authors_to_books:
    input:
      postgres_cdc:
        url: "postgres://user:pass@localhost/app"
        publication: "authors_pub"
    output:
      middlewares:
        - transform:
            mapping: { id: "$.id", name: "$.name" }
      custom:
        name: meilisearch
        config:
          url: "http://localhost:7700"
          index: "books"
          primary_key: "id"
          update_where: "author.id = ${payload:id}"
          update_into: author
```

For each message the sink asks Meilisearch for the ids of the documents where
`author.id` equals the message's `id` (`POST /documents/fetch` with a filter,
Meilisearch 1.14+), then merges `{"id": <book id>, "author": <message>}` into
each of them, split by `max_request_bytes`. The message replaces the `author`
field whole, so shape it with a `transform` into what the `lookup` wrote. A row
no document refers to is acknowledged without a write.

- The attribute must be in the index's `filterableAttributes`; otherwise
  Meilisearch refuses the filter and the message is dead-lettered.
- Only equality is supported. The sink quotes the value itself, so a value read
  from a message cannot widen the filter.
- The route never deletes: it takes no `operation`, and a parent row's delete
  arrives as one more row to write. Drop deletes with a `filter` middleware, or
  let the foreign key decide what happens to the children at the source.
- A change costs one write per matching document, so a parent with a million
  children is a million updates. Before each batch the route waits for the
  writes already queued on the index, as `update_existing` does.
- One gap remains: a child written by the other route with a `lookup` that read
  the parent just before the change, and indexed just after this route matched,
  keeps the old value until either row changes again.

**Deletes remove the whole document.** With `operation` mapped, deleting a
`movie_stats` row deletes the movie, not just its rating. Map `operation` only
on the route that owns the document (`movies`) and leave it unset on the others:
there a delete becomes an update carrying the key with every other column
`null` (under the default replica identity), so the rating is cleared rather
than the movie removed. Under `REPLICA IDENTITY FULL` the delete carries the
whole old row, so it writes the old values back instead. Routes are not ordered
against each other, and `update` creates a document that does not exist yet, so
a `movie_stats` change landing after its movie was deleted leaves a stub
document holding only those fields.

**`method: update_existing` prevents those stubs** (0.1.3+). Use it on the routes that
only add to a document (`movie_stats` above), not on the one that owns it.
Before each write it waits for the writes already queued on the index, so a
parent another route just created or deleted is seen, then asks Meilisearch
which of the batch's documents exist (`POST /documents/fetch`, Meilisearch
1.14+), merges into those and acknowledges the rest unwritten. The wait ties
the route's pace to the index's indexing speed. It needs `primary_key` and
cannot be combined with `operation`. One gap remains: a parent row read
before its child committed but written to Meilisearch after the check misses
that child until the child changes again.

## Composite primary keys

Meilisearch keys a document by one field. For a table keyed by several columns,
build that field with a `transform` expression; a delete still carries the key
columns, so it names the same document:

```yaml
output:
  middlewares:
    - transform:
        expression: >-
          { id: string(order_id) + '-' + string(line_no),
            order_id: order_id, line_no: line_no, sku: sku, qty: qty }
  custom:
    name: meilisearch
    config:
      url: "http://localhost:7700"
      index: "order_lines"
      primary_key: "id"
      operation: "${metadata:postgres.operation}"
```

The expression returns the whole document, so list every field to keep. The
parts may only use `a-z A-Z 0-9 - _`, Meilisearch's rule for an id. `capture_all`
cannot back fill such a table, since it pages by a single key column: stream it
with the default `capture_new`, or back fill it through a `sequence` with a
unique `cursor_column`.

## Rebuilding an index

After changing a `transform` or the index's `settings`, re-read the table. One
command does it for a document that maps 1:1 to a table:

```console
mq-bridge-app copy --drain \
  'postgres://user:pass@localhost/app?table=movies&cursor_column=id' \
  'meilisearch://localhost:7700?index=movies&primary_key=id&api_key=…'
```

A route with `lookup`s re-reads the same way from a config: run the owning
route once more with a new `slot_name` and `checkpoint_store` and
`consume: capture_all`, then drop the old slot with
`SELECT pg_drop_replication_slot('…')` — it retains WAL until dropped.

**Without downtime,** build the new version under another name and let
`swap_into` put it live (0.1.3+, mq-bridge 0.4.18+). Searches keep hitting the
old index until the swap, which is atomic:

1. Stop the streaming route. Its slot keeps the changes that arrive meanwhile.
2. Rebuild into another index, naming the live one in `swap_into`. Give the
   rebuilding endpoint the index's `settings`: a new index starts without them.

   ```console
   mq-bridge-app copy --drain \
     'postgres://user:pass@localhost/app?table=movies&cursor_column=id' \
     'meilisearch://localhost:7700?index=movies_v2&swap_into=movies&primary_key=id&api_key=…'
   ```

3. Start the streaming route again. It replays the changes from step 1 into the
   rebuilt index.

When the route has drained its input, the endpoint swaps `movies_v2` with
`movies` (`POST /swap-indexes`) and deletes `movies_v2`, which then holds the
old documents. It swaps only when all of this holds, and otherwise logs a
warning and leaves `movies` untouched:

- the input was read to its end — a route that was stopped, or failed, has
  built only part of the index;
- every document was written — one that was refused or dead-lettered blocks the
  swap, since the new index would be missing it, while one that failed and was
  then written by a `retry` does not;
- at least one document was written, so an empty source cannot blank the live
  index.

A swap that is skipped or fails **does not fail the route**: mq-bridge only
logs an error raised while a route shuts down, so `copy --drain` still exits
`0`. Check the log for "is now live" before relying on it. Under a host older
than mq-bridge 0.4.18, which does not tell a plugin how the route ended, the
swap is always skipped; do it by hand with `POST /swap-indexes` and
`[{"indexes": ["movies", "movies_v2"]}]`, then `DELETE /indexes/movies_v2`.

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

`--plugin` only works on a host that does not already have the endpoint. The
`mqb` CLI ships with a copy built in (0.1.2 in mqb 0.4.17) and refuses a second
one named on the command line as "already registered". To run a newer build
there, point the plugin search path at it and ask for the replacement:

```console
MQB_PLUGIN_DIR=./target/release MQB_PLUGIN_OVERRIDE=meilisearch mqb copy -v …
```

With `-v` the log confirms it: "using the installed `meilisearch` plugin …
instead of the built-in copy".

The library is built against plugin ABI 1.3 and loads in mq-bridge 0.4.13 or
newer; `swap_into` is the one feature that needs a 0.4.18 host. A URI query carries no types, so the host maps one against the schema
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

- **Joined fields go stale when the parent changes, unless a route pushes the
  change.** A `lookup` reads the referenced row when the child is written; a
  later change to that row reaches existing documents only through a second
  route with `update_where`. See [Merging rows](#merging-rows-from-different-tables).
- **`update` merges top-level fields only**: `{"tags": ["a"]}` replaces the whole
  array. Meilisearch's function-based edit applies to a filtered document set
  rather than one document per message, so it does not fit this sink. Compute
  the value upstream.
- **No replay log.** Recovering from a bad transform means re-reading the
  source; see [Rebuilding an index](#rebuilding-an-index).
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
