# mq-bridge-meilisearch

The [Meilisearch](https://www.meilisearch.com/) endpoint for
[mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

This package contains no Python implementation of Meilisearch. It carries the
compiled Rust plugin and hands its path to mq-bridge's generic loader, so the
endpoint behaves identically here, in Node.js and in a Rust host.

```console
pip install mq-bridge-py mq-bridge-meilisearch
```

```python
import mq_bridge
import mq_bridge_meilisearch

mq_bridge_meilisearch.register()   # once, before starting routes

mq_bridge.Route.from_str("""
input:
  file: { path: "movies.jsonl", format: raw }
output:
  custom:
    name: meilisearch
    config:
      url: "http://localhost:7700"
      api_key: "masterKey"
      index: "movies"
      primary_key: "id"
exit_on_empty: true
""").run()
```

`register()` is idempotent and returns the endpoint name, `"meilisearch"`.
`library_path()` returns the absolute path of the bundled library, for a host
that would rather call `load_endpoint_plugin` itself.

Use mq-bridge 0.4.13 or newer: the plugin speaks ABI 1.1, which lets it ask the
route to keep its sends in source order at any `concurrency` — Meilisearch keys
documents by primary key, so two batches touching one document must not be
reordered.

The mq-bridge dependency is deliberately not declared: the two packages are
installed and upgraded independently, and `register()` reports a clear error if
mq-bridge is absent.

Full configuration reference, the Postgres CDC recipe and the cross-table merge
recipe are in the
[project README](https://github.com/marcomq/mq-bridge-meilisearch#readme).

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
