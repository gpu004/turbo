import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

import turboPlugin, { RULE_OWNERS } from "./index.ts";

// Tests for the turbo lint rules, run by `npm run check`.
//
// 1. Each fixture marks the lines a rule must flag with `// expect: turbo/<rule>`.
//    Oxlint runs with the real `.oxlintrc.json`, so the configured options are
//    tested too. Every marked line must be flagged and nothing else may be.
// 2. Every rule the plugin exports is enabled, and `.oxlintrc.json` turns a
//    rule off only for the files `RULE_OWNERS` lists.

const FIXTURES = "tools/oxlint/turbo/fixtures";

const MARKER = /\/\/ expect: (turbo\/[a-z-]+)/;

interface Diagnostic {
  code: string;
  filename: string;
  labels: Array<{ span: { line: number } }>;
}

interface OverrideConfig {
  files: string[];
  rules?: Record<string, unknown>;
}

interface LintConfig {
  rules: Record<string, unknown>;
  overrides?: OverrideConfig[];
}

const failures: string[] = [];

function expectedDiagnostics(): Set<string> {
  const expected = new Set<string>();

  for (const name of readdirSync(FIXTURES)) {
    const file = join(FIXTURES, name);

    for (const [index, line] of readFileSync(file, "utf8").split("\n").entries()) {
      const rule = MARKER.exec(line)?.[1];

      if (rule) expected.add(`${file}:${index + 1} ${rule}`);
    }
  }

  return expected;
}

function actualDiagnostics(): Set<string> {
  const run = spawnSync("npx", ["oxlint", "-c", ".oxlintrc.json", "-f", "json", FIXTURES], {
    encoding: "utf8",
  });

  if (run.error) throw run.error;

  const output: { diagnostics: Diagnostic[] } = JSON.parse(run.stdout);
  const actual = new Set<string>();

  for (const diagnostic of output.diagnostics) {
    const rule = /^turbo\((.+)\)$/.exec(diagnostic.code)?.[1];
    const line = diagnostic.labels[0]?.span.line;

    if (rule && line !== undefined) actual.add(`${diagnostic.filename}:${line} turbo/${rule}`);
  }

  return actual;
}

const expected = expectedDiagnostics();
const actual = actualDiagnostics();

for (const entry of expected) {
  if (!actual.has(entry)) failures.push(`not flagged: ${entry}`);
}

for (const entry of actual) {
  if (!expected.has(entry)) failures.push(`flagged without an expect marker: ${entry}`);
}

const config: LintConfig = JSON.parse(readFileSync(".oxlintrc.json", "utf8"));

for (const rule of Object.keys(turboPlugin.rules)) {
  if (config.rules[`turbo/${rule}`] === undefined) {
    failures.push(`turbo/${rule} is not enabled in .oxlintrc.json`);
  }

  if (!RULE_OWNERS[rule]) failures.push(`turbo/${rule} has no entry in RULE_OWNERS`);
}

for (const override of config.overrides ?? []) {
  for (const file of override.files) {
    if (!existsSync(file)) failures.push(`.oxlintrc.json override names missing file ${file}`);
  }

  for (const [name, setting] of Object.entries(override.rules ?? {})) {
    const rule = /^turbo\/(.+)$/.exec(name)?.[1];

    if (!rule || setting !== "off") continue;

    const owners = RULE_OWNERS[rule] ?? [];

    for (const file of override.files) {
      if (!owners.includes(file)) {
        failures.push(
          `.oxlintrc.json turns off ${name} for ${file}, which RULE_OWNERS does not list. Move the code into ${owners.join(" or ")} instead.`,
        );
      }
    }
  }
}

if (failures.length > 0) {
  for (const failure of failures) console.error(`✗ ${failure}`);
  process.exit(1);
}

console.log(`turbo lint rules: ${expected.size} expected diagnostics, config matches RULE_OWNERS`);
