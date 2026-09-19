"use strict";

const plugin = require("mq-bridge").definePluginPackage(__dirname);

// Assigned name by name, not just `module.exports = plugin`: Node's CommonJS
// lexer only detects exports it can see statically, and without them
// `import { register } from "mq-bridge-meilisearch"` fails in ESM.
module.exports = plugin;
module.exports.ENDPOINT_NAME = plugin.ENDPOINT_NAME;
module.exports.libraryPath = plugin.libraryPath;
module.exports.register = plugin.register;
