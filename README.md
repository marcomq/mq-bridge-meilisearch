# mq-bridge-meilisearch

An external [mq-bridge](https://github.com/marcomq/mq-bridge) endpoint for
[Meilisearch](https://www.meilisearch.com/), talking to its REST API directly.
It works as an **output** (a document sink, which is what streaming database
changes into a search index needs) and as an **input** (a non-destructive scan
of an index's documents).

It adds no Meilisearch dependency to mq-bridge itself: link it as a crate, or
load the compiled library as a plugin from any mq-bridge host, including the
Python and Node.js packages.

## Configuration

Register the endpoint before any route starts:

```rust
mq_bridge_meilisearch::register()?;
```

Then use the explicit custom endpoint form:

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
| `url` | both | *required* | Base URL, e.g. `http://localhost:7700`. |
| `api_key` | both | none | Sent as `Authorization: Bearer`. Omit for an instance with no master key. |
| `index` | both | route name | Index UID. |
| `primary_key` | both | none | The field Meilisearch keys documents by. |
| `method` | output | `replace` | `replace` overwrites a document; `update` merges top-level fields into it. |
| `operation` | output | none | Template naming each message's change operation, e.g. `${metadata:postgres.operation}`. |
| `delete_values` | output | `["delete"]` | Operation values that mean "remove this document". |
| `create_index` | both | `true` | Create the index at startup so `primary_key` applies before the first document. |
| `wait_for_task` | output | `true` | Wait for the indexing task to finish before acknowledging. |
| `task_timeout_ms` | output | `60000` | How long to wait for one task. |
| `connect_timeout_ms` | both | `10000` | TCP/TLS connect timeout. |
| `request_timeout_ms` | both | none | Whole-request timeout. |
| `fields` | input | all | Comma-separated document fields to read. |
| `cursor_id` | input | none | Names this reader's position; without it every restart re-reads from the first document. |
| `checkpoint_store` | input | none | Where that position is persisted: a `file://` spec or a plain path. |
| `polling_interval_ms` | input | `1000` | Delay between polls once the scan has reached the end. |
| `max_polling_interval_ms` | input | none | Upper bound the idle delay backs off to. |

Set `primary_key` whenever you can. Left unset, Meilisearch infers it from the
first batch by picking a field whose name contains `id`, which can silently pick
the wrong one — and the inference is permanent for the life of the index.

## Streaming Postgres changes into an index

This is the case the endpoint is built for. Messages are already JSON objects,
so a plain table copy needs no mapping at all:

```yaml
routes:
  movies_to_search:
    batch_size: 1000
    input:
      postgres:
        url: "postgres://user:pass@localhost/app"
        publication: "movies_pub"
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

With `operation` set, each message is classified as an upsert or a delete.
Postgres CDC emits `insert`, `update`, `delete` and `truncate`; `delete_values`
decides which of those remove a document, and everything else is an upsert.
A `truncate` carries no row, so it is reported as non-retryable and goes to the
route's dead-letter queue rather than being indexed as an empty document.

Without `operation`, every message is an upsert — which is all a one-off table
copy or a file import needs.

### How a batch becomes requests

A batch is split into contiguous runs of the same operation, and the runs are
issued in order. A CDC batch is normally all one operation, so it becomes a
single HTTP request; upserts are sent as NDJSON built by concatenating the
payload bytes, with no re-serialization.

The ordering is load-bearing. Reordering `insert(id=7)` and `delete(id=7)` from
one batch would leave document 7 in the index permanently, so runs are never
merged or reordered, and a run that fails takes every later run in the batch
with it — the route never sees a write it did not issue reported as delivered.

### Acknowledgement

Meilisearch answers a write with `202 Accepted` and a task id, then applies it
afterwards. Acknowledging on that 202 would commit the source's replication slot
for a write that can still fail (`missing_document_id`, `invalid_document_id`,
`payload_too_large`), losing the row silently.

So by default `wait_for_task: true` polls the task to a finished state, and a
failed task is reported back to the route for retry or dead-lettering. That is
one extra round trip per batch — negligible at `batch_size: 1000`. Set it to
`false` only if you would rather lose a row than wait.

### Ordering and `concurrency`

Meilisearch keys documents by primary key, so two batches touching one document
must arrive in source order. Whether you get that depends on how the endpoint is
loaded and on how the route was started.

**Linked as a crate you are safe at any `concurrency`.** The endpoint declares
`requires_ordered_publish`, so the route keeps sends sequenced while running
everything around them across the worker pool.

**Loaded as a plugin you are not.** Plugin ABI 1.0 has no vtable slot for that
flag — the publisher half is `create`/`send_batch`/`flush`/`close`/`free` — so
the host falls back to the trait default of "unordered" and publishes batches in
parallel. What that costs depends on the default in front of you, which is not
the same everywhere:

| How the route is started | `concurrency` default | Plugin-loaded sink |
| --- | --- | --- |
| A YAML route file | `1` | safe as-is |
| `mq-bridge copy …` | `4` | **pass `--concurrency 1`** |
| MCP `start_route` / `route_messages` | `4` | **set `"concurrency": 1`** |

Out of order, two updates to one document resolve last-write-wins, so an older
row can overwrite a newer one with nothing in the log to say so.

The consumer side has no such gap: `commit_requires_order` *is* an ABI entry, so
the scan's cumulative offset is committed in order either way.

## Merging rows from different tables

Meilisearch's `PUT /documents` merges top-level fields into an existing
document. One route per source table, all writing the same `primary_key` with
`method: update`, therefore makes Meilisearch perform the join:

```yaml
routes:
  movies:
    input:
      postgres: { url: "postgres://...", publication: "movies_pub" }
    output:
      custom:
        name: meilisearch
        config: { url: "http://localhost:7700", index: "movies", primary_key: "id", method: update }

  directors:
    input:
      postgres: { url: "postgres://...", publication: "directors_pub" }
    output:
      custom:
        name: meilisearch
        config: { url: "http://localhost:7700", index: "movies", primary_key: "id", method: update }
```

A row from `movies` and a row from `directors` sharing `id: 1` become one
document carrying both sets of fields. Use a `transform` middleware first when
the shapes need reshaping — for example to rename a joined table's `name` to
`director_name`, or to nest it under its own key.

Note that `update` merges **top-level fields only**: writing `{"id": 1, "tags":
["a"]}` replaces the whole `tags` array rather than appending to it.

## Reading an index back out

As an input, the endpoint pages through `GET /indexes/{uid}/documents` and emits
each document as one message, with `meilisearch.index` and (when `primary_key`
is set) `meilisearch.document_id` metadata.

```yaml
input:
  custom:
    name: meilisearch
    config:
      url: "http://localhost:7700"
      index: "movies"
      primary_key: "id"
      cursor_id: "export"
      checkpoint_store: "file:///var/lib/mq-bridge/meilisearch.json"
output:
  file:
    path: "movies.jsonl"
```

`/documents` rather than `/search`: search pagination stops at `maxTotalHits`
(1000 by default), so it cannot export a whole index, while this endpoint is
uncapped.

Two things to know:

- **Paging is by offset**, which is stable only for an index nobody is writing
  to. Documents inserted or removed during a long scan can be skipped or
  repeated. For an export, drain a quiet index.
- **`checkpoint_store` accepts a file path only.** mq-bridge's SQL, Mongo and
  object-store checkpoint backends are compiled into mq-bridge behind its own
  feature flags, and a plugin `cdylib` links its own copy of mq-bridge with
  those off — a `postgres://` checkpoint could never be reached from inside the
  plugin, so this endpoint does not pretend to offer one.

A nacked batch rolls the scan position back to the last acknowledged document.
A batch dropped *without* committing does not roll back, the same as mq-bridge's
other cursor-paged readers: the position advances when the page is read and is
corrected only by a commit.

## Example

With a Meilisearch instance listening on localhost:

```console
cargo run --features example-app --example file_to_meilisearch
```

The runnable route is in `examples/file_to_meilisearch.yaml`; it loads the three
documents in `examples/movies.jsonl` into an index.

## Use it from any mq-bridge process

The crate also builds a `cdylib` — the same endpoint as a native plugin — so a
host that never compiled against it can load it at runtime:

```rust
mq_bridge::plugin::load_endpoint_plugin("./libmq_bridge_meilisearch.so")?;
```

Python and Node.js users install two independent packages; neither reimplements
Meilisearch, both ship this library and hand its path to mq-bridge's generic
loader.

```console
pip install mq-bridge mq-bridge-meilisearch
```

```python
import mq_bridge_meilisearch

mq_bridge_meilisearch.register()   # once, before starting routes
```

```console
npm install mq-bridge mq-bridge-meilisearch
```

```javascript
import { register } from "mq-bridge-meilisearch";

register(); // once, before starting routes
```

The configuration is the same in every language (`name: meilisearch`). See
[PLUGINS.md](https://github.com/marcomq/mq-bridge/blob/main/docs/PLUGINS.md) for
how loading, versioning and the ABI work.

### Packaging

Python publishes one platform wheel per target under the same distribution
name. The npm release is a single package containing all staged binaries under
`node/prebuilds/`. Build on each target, merge those directories, then pack once:

```console
pip install "mq-bridge-py[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_meilisearch --out dist
mq-bridge-package-plugin
mq-bridge-package-plugin --pack --out npm
```

`Cargo.toml` is the source of truth for the package version. Update every
ecosystem manifest together before tagging a release:

```console
python3 scripts/set_version.py 0.1.1
```

CI checks that the Cargo, npm and Python versions remain synchronized.

## Tests

```console
cargo test --lib
cargo test --test integration -- --ignored --nocapture
cargo test --test plugin -- --ignored --nocapture
```

The unit tests need nothing. Both other files start a Meilisearch container from
`tests/docker-compose.yml`, which is why they are ignored by default.

`integration.rs` covers the directly linked endpoint: an upsert/delete round
trip, an insert and a delete of one key in a single batch, the cross-table merge
above, a task that fails *after* being accepted, a nacked batch being re-read,
and a checkpointed scan resuming where it stopped.

`plugin.rs` runs one suite twice against the same instance — once against the
directly linked factory, once against the factory loaded from the compiled
plugin — and requires the results to match. mq-bridge's own
`plugin::conformance` suite is not used: its checks publish plain-string
payloads and compare them byte for byte, while Meilisearch stores JSON documents
and returns them re-serialized from its own store.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
