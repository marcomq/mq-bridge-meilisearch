"""End-to-end tests for the Meilisearch endpoint as Python loads it: a native plugin.

This is the `cdylib` path — `register()` hands the bundled library to
mq-bridge's generic loader — so it exercises the plugin ABI, not the directly
linked factory that `cargo test --test integration` covers. Both must agree.

Run against the instance the Rust tests use:

    docker compose -f tests/docker-compose.yml up -d
    pip install mq-bridge-py mq-bridge-meilisearch   # or a locally built wheel
    pytest python/tests -v
    docker compose -f tests/docker-compose.yml down

Every test skips (rather than fails) when the packages are missing or nothing is
listening, so the file is safe to collect in an environment without either.
"""

import json
import socket
import urllib.request
import uuid

import pytest

MEILI_HOST, MEILI_PORT = "localhost", 7700
MEILI_URL = f"http://{MEILI_HOST}:{MEILI_PORT}"
API_KEY = "mq-bridge-test-key"
ROWS = 100

mq_bridge = pytest.importorskip("mq_bridge", reason="mq-bridge-py is not installed")
mq_bridge_meilisearch = pytest.importorskip(
    "mq_bridge_meilisearch", reason="mq-bridge-meilisearch is not installed"
)


def _instance_is_up() -> bool:
    try:
        with socket.create_connection((MEILI_HOST, MEILI_PORT), timeout=2):
            return True
    except OSError:
        return False


requires_meilisearch = pytest.mark.skipif(
    not _instance_is_up(),
    reason=f"no Meilisearch on {MEILI_HOST}:{MEILI_PORT} "
    "(docker compose -f tests/docker-compose.yml up -d)",
)


@pytest.fixture(scope="session", autouse=True)
def registered():
    """Registration is process-global, so do it once for the whole session."""
    assert mq_bridge_meilisearch.register() == "meilisearch"
    return "meilisearch"


@pytest.fixture
def index() -> str:
    """A fresh index per test, so no test can see another's documents."""
    return f"pytest-{uuid.uuid4().hex[:10]}"


@pytest.fixture
def source_file(tmp_path):
    path = tmp_path / "in.jsonl"
    with path.open("w") as handle:
        for i in range(1, ROWS + 1):
            handle.write(json.dumps({"id": i, "title": f"movie-{i}"}) + "\n")
    return path


def _endpoint(index: str, **extra) -> str:
    """The `custom` form is how every non-Rust host addresses a plugin."""
    config = {
        "url": MEILI_URL,
        "api_key": API_KEY,
        "index": index,
        "primary_key": "id",
        **extra,
    }
    lines = "\n".join(f"        {k}: {json.dumps(v)}" for k, v in config.items())
    return f"    custom:\n      name: meilisearch\n      config:\n{lines}"


def _load(source_file, index: str, **extra) -> None:
    mq_bridge.Route.from_str(
        f"""
input:
  file: {{ path: "{source_file}", format: raw }}
output:
{_endpoint(index, **extra)}
exit_on_empty: true
"""
    ).run()


def _drain_to(out_path, index: str, **extra) -> int:
    mq_bridge.Route.from_str(
        f"""
input:
{_endpoint(index, **extra)}
output:
  file: {{ path: "{out_path}", format: json }}
exit_on_empty: true
"""
    ).run()
    if not out_path.exists():
        return 0
    with out_path.open() as handle:
        return sum(1 for _ in handle)


def _document_count(index: str) -> int:
    request = urllib.request.Request(
        f"{MEILI_URL}/indexes/{index}/documents?limit=0",
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)["total"]


def test_register_is_idempotent(registered):
    """Calling it again is a no-op, not the 'already registered' error."""
    assert mq_bridge_meilisearch.register() == "meilisearch"


def test_library_path_points_at_a_real_file():
    from pathlib import Path

    assert Path(mq_bridge_meilisearch.library_path()).is_file()


@requires_meilisearch
def test_every_row_is_indexed(source_file, index):
    _load(source_file, index)
    assert _document_count(index) == ROWS


@requires_meilisearch
def test_the_index_can_be_read_back_out(source_file, tmp_path, index):
    _load(source_file, index)
    out = tmp_path / "out.jsonl"

    assert _drain_to(out, index) == ROWS

    ids = [json.loads(line)["payload"]["id"] for line in out.read_text().splitlines()]
    assert sorted(ids) == list(range(1, ROWS + 1))


@requires_meilisearch
def test_update_merges_a_second_table_into_the_same_documents(source_file, tmp_path, index):
    """Meilisearch performs the join: one route per table, one primary key."""
    _load(source_file, index)

    directors = tmp_path / "directors.jsonl"
    with directors.open("w") as handle:
        for i in range(1, ROWS + 1):
            handle.write(json.dumps({"id": i, "director": f"director-{i}"}) + "\n")
    _load(directors, index, method="update")

    out = tmp_path / "merged.jsonl"
    assert _drain_to(out, index) == ROWS
    first = json.loads(out.read_text().splitlines()[0])["payload"]
    assert first["title"].startswith("movie-")
    assert first["director"].startswith("director-")


@requires_meilisearch
def test_a_rejected_config_surfaces_as_an_error(tmp_path, index):
    """A config the endpoint rejects must reach the caller, not hang.

    Scope: this proves the error *surfaces*. It does not prove the ABI status
    was classified as permanent, because `run()` on a drain route also raises
    via the startup timeout when the failure is merely retryable — so this test
    passes either way. The classification itself is asserted where it is
    observable: `plugin::endpoint` unit tests in the mq-bridge repo, and the
    directly linked path in `tests/integration.rs`.
    """
    route = mq_bridge.Route.from_str(
        f"""
input:
{_endpoint(index, definitely_not_a_field="x")}
output:
  file: {{ path: "{tmp_path / 'never.jsonl'}" }}
exit_on_empty: true
"""
    )
    with pytest.raises(
        Exception, match="unknown field|invalid Meilisearch endpoint configuration"
    ):
        route.run()
