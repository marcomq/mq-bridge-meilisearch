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
A message that leaves one unresolved is dead-lettered rather than written
somewhere arbitrary.

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

A batch is split into contiguous runs sharing one operation and one index, and
the runs are issued in order — reordering `insert(id=7)` and `delete(id=7)`
would leave document 7 in the index for good. A failing run takes every later
run with it, so the route never sees an unissued write reported as delivered. A
run over `max_request_bytes` is split into several ordered requests rather than
rejected as `payload_too_large`.

Meilisearch answers a write with `202 Accepted` and applies it afterwards, so
`wait_for_task` polls the task to a finished state before acknowledging.
Otherwise the source's replication slot advances past writes that can still fail
(`missing_document_id`, `invalid_document_id`), losing rows silently. That is one
round trip per batch — negligible at `batch_size: 1000`.

### Ordering and `concurrency`

Two batches touching one document must arrive in source order, or an older row
overwrites a newer one silently. **Linked as a crate, you are safe at any
`concurrency`**: the endpoint declares `requires_ordered_publish`.

**Loaded as a plugin, you are not** — plugin ABI 1.0 has no vtable slot for that
flag, so the host publishes batches in parallel. Until an ABI carrying it ships,
a plugin-loaded sink needs `concurrency: 1` wherever the default is higher
(`mqb copy` and MCP `start_route` default to 4; a YAML route file defaults to 1).
The input side is unaffected: `commit_requires_order` *is* an ABI entry.

## Backfilling an existing table

CDC carries changes, not existing rows. Create the slot **before** the copy, or
changes made during it are lost:

```sql
select pg_create_logical_replication_slot('mqb_meili', 'pgoutput');
create publication movies_pub for table public.movies;
```

```console
# 2. copy the rows that are already there
mqb copy --drain --plugin ./libmq_bridge_meilisearch.so \
  'postgres://user:pass@host:5432/app?table=public.movies' \
  'meilisearch://localhost:7700?index=movies&primary_key=id&api_key=KEY'

# 3. stream the changes the slot has been holding since step 1
mqb copy --plugin ./libmq_bridge_meilisearch.so \
  'postgres-cdc://user:pass@host:5432/app?publication=movies_pub&slot_name=mqb_meili' \
  'meilisearch://localhost:7700?index=movies&primary_key=id&api_key=KEY&operation=${metadata:postgres.operation}'
```

Both halves are keyed by `primary_key` and therefore idempotent, so the replay
corrects anything the copy applied stale. Watch the slot's lag while step 2
runs: an unread slot retains WAL.

A URI query carries no types, so every option arrives as a string;
`create_index=false`, `task_timeout_ms=30000` and `delete_values=delete,remove`
are all accepted. YAML keeps using typed forms.

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
Meilisearch perform the join. A row from `movies` and a row from `directors`
sharing `id: 1` become one document. Use a `transform` middleware first if the
shapes need reshaping.

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

- **Backfill is a separate step**, not a snapshot phase the route hands over
  from, and it is not resumable. This belongs in mq-bridge's Postgres source.
- **`update` merges top-level fields only**: `{"tags": ["a"]}` replaces the whole
  array. Meilisearch's function-based edit applies to a filtered document set
  rather than one document per message, so it does not fit this sink. Compute
  the value upstream.
- **No replay log.** Recovering from a bad transform means re-running the
  backfill. Put a durable queue in front of the sink if you need better.
- **`checkpoint_store` takes a file path only.** mq-bridge's SQL, Mongo and
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
