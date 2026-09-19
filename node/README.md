# mq-bridge-meilisearch

The [Meilisearch](https://www.meilisearch.com/) endpoint for
[mq-bridge](https://github.com/marcomq/mq-bridge), shipped as a native plugin.

This package contains no JavaScript implementation of Meilisearch. It carries
the compiled Rust plugin for your platform and hands its path to mq-bridge's
generic loader, so the endpoint behaves identically here, in Python and in a
Rust host.

```console
npm install mq-bridge mq-bridge-meilisearch
```

```javascript
import { Route } from "mq-bridge";
import { register } from "mq-bridge-meilisearch";

register(); // once, before starting routes

await Route.fromString(`
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
`).run();
```

`register()` is idempotent and returns the endpoint name, `"meilisearch"`.
`libraryPath()` returns the absolute path of the bundled library, for a host
that would rather load it itself.

Run a route using this plugin at `concurrency` 1. Meilisearch keys documents by
primary key, so two batches touching one document must arrive in source order,
and plugin ABI 1.0 has no slot for the flag that would let the endpoint ask the
route to keep its sends sequenced. A YAML route already defaults to 1, but
`mq-bridge copy` and the MCP route tools default to 4 — pass `--concurrency 1`
or `"concurrency": 1` there.

mq-bridge is a peer dependency: the two packages are installed and upgraded
independently.

Full configuration reference, the Postgres CDC recipe and the cross-table merge
recipe are in the
[project README](https://github.com/marcomq/mq-bridge-meilisearch#readme).

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your
option.
