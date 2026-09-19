// The README tells users to `import { register } from "mq-bridge-meilisearch"`.
// That named import only resolves if the CommonJS entrypoint exports it
// statically, so this imports the package the same way a consumer does.
import assert from "node:assert/strict";
import test from "node:test";

import { ENDPOINT_NAME, libraryPath, register } from "mq-bridge-meilisearch";

test("the package entrypoint exposes register as a named ESM import", () => {
  assert.equal(typeof register, "function");
  assert.equal(typeof libraryPath, "function");
  assert.equal(ENDPOINT_NAME, "meilisearch");
});
