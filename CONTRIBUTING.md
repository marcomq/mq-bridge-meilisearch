# Contributing

## Building and testing

```console
cargo test --lib                                     # no dependencies
cargo test --test integration -- --ignored           # starts a Meilisearch container
cargo test --test plugin -- --ignored                # same suite, direct vs plugin-loaded
cargo clippy --all-targets --all-features
cargo fmt --check
```

The Docker-backed files start Meilisearch from `tests/docker-compose.yml`. With
an instance already listening on localhost, the runnable example in
`examples/file_to_meilisearch.yaml` loads three documents into an index:

```console
cargo run --features example-app --example file_to_meilisearch
```

`integration.rs` covers the directly linked endpoint: an upsert/delete round
trip, an insert and a delete of one key in a single batch, a cross-table merge,
one batch fanned across two indexes by a template, `settings` reaching the index
before the first document, a batch split across several requests still landing
complete, a task that fails *after* being accepted, a nacked batch being
re-read, a checkpointed scan resuming where it stopped, and the config shape the
CLI builds from a `meilisearch://` URI.

## Packaging

The crate builds a `cdylib` that any mq-bridge host can load. Python publishes
one platform wheel per target under a single distribution name; the npm release
is one package holding every staged binary under `node/prebuilds/`. Build on
each target, merge those directories, then pack once:

```console
pip install "mq-bridge-py[plugin-packaging]"
python -m mq_bridge.plugin_packaging --package python/mq_bridge_meilisearch --out dist
mq-bridge-package-plugin
mq-bridge-package-plugin --pack --out npm
```

## Releasing

`Cargo.toml` is the source of truth for the version. Update every ecosystem
manifest together before tagging, and CI checks they stay synchronized:

```console
python3 scripts/set_version.py 0.1.1
```

Pushing a tag that starts with a digit (`0.1.1`, not `v0.1.1`) runs
`.github/workflows/release.yml`, which builds all five platforms and publishes
to crates.io, npm and PyPI over OIDC trusted publishing — no long-lived tokens.
Each publishing job needs a GitHub deployment environment of the matching name
(`crates-io`, `npm`, `pypi`).
