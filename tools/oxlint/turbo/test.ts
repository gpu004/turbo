import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import turboPlugin, { RULE_OWNERS } from "./index.ts";

const FIXTURES = "tools/oxlint/turbo/fixtures";

const MARKER = /\/\/ expect: (turbo\/[a-z-]+)/;

interface Diagnostic {
  code: string;
  filename: string;
  message: string;
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

function runOxlint(configPath: string, target: string): Diagnostic[] {
  const run = spawnSync("npx", ["oxlint", "-c", configPath, "-f", "json", target], {
    encoding: "utf8",
  });

  if (run.error) throw run.error;

  const output: { diagnostics: Diagnostic[] } = JSON.parse(run.stdout);

  return output.diagnostics;
}

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
  const actual = new Set<string>();

  for (const diagnostic of runOxlint(".oxlintrc.json", FIXTURES)) {
    const rule = /^turbo\((.+)\)$/.exec(diagnostic.code)?.[1];
    const line = diagnostic.labels[0]?.span.line;

    if (rule && line !== undefined) actual.add(`${diagnostic.filename}:${line} turbo/${rule}`);
  }

  return actual;
}

function verifyNoCommentsRatchet(): void {
  const fixture = join(FIXTURES, "no-comments.ts");
  const tempDir = mkdtempSync(join(tmpdir(), "turbo-lint-ratchet-"));

  try {
    const runWithBudget = (budget: number): Diagnostic[] => {
      const configPath = join(tempDir, `ratchet-${budget}.json`);

      writeFileSync(
        configPath,
        JSON.stringify({
          jsPlugins: [
            { name: "turbo", specifier: join(process.cwd(), "tools/oxlint/turbo/index.ts") },
          ],
          rules: {
            "turbo/no-comments": ["error", { maxCommentsByFile: { [fixture]: budget } }],
          },
        }),
      );

      return runOxlint(configPath, fixture).filter((d) => d.code === "turbo(no-comments)");
    };

    const overBudget = runWithBudget(2);
    const overLines = overBudget.map((d) => d.labels[0]?.span.line);

    if (overLines.join(",") !== "3,4") {
      failures.push(`ratchet budget=2 expected lines 3,4 but got ${overLines.join(",") || "none"}`);
    }

    const atBudget = runWithBudget(4);

    if (atBudget.length !== 0) {
      failures.push(`ratchet budget=4 expected 0 diagnostics but got ${atBudget.length}`);
    }

    const underBudget = runWithBudget(5);

    if (underBudget.length !== 1 || !underBudget[0]?.message.includes("Lower maxCommentsByFile")) {
      failures.push(
        `ratchet budget=5 expected 1 tightenRatchet diagnostic but got ${underBudget.length}`,
      );
    }
  } finally {
    rmSync(tempDir, { recursive: true, force: true });
  }
}

const expected = expectedDiagnostics();
const actual = actualDiagnostics();

for (const entry of expected) {
  if (!actual.has(entry)) failures.push(`not flagged: ${entry}`);
}

for (const entry of actual) {
  if (!expected.has(entry)) failures.push(`flagged without an expect marker: ${entry}`);
}

verifyNoCommentsRatchet();

const config: LintConfig = JSON.parse(readFileSync(".oxlintrc.json", "utf8"));

for (const rule of Object.keys(turboPlugin.rules)) {
  if (config.rules[`turbo/${rule}`] === undefined) {
    failures.push(`turbo/${rule} is not enabled in .oxlintrc.json`);
  }

  if (!RULE_OWNERS[rule]) failures.push(`turbo/${rule} has no entry in RULE_OWNERS`);
}

const noCommentsSetting = config.rules["turbo/no-comments"];

if (Array.isArray(noCommentsSetting)) {
  const option = noCommentsSetting[1];

  if (typeof option === "object" && option !== null && "maxCommentsByFile" in option) {
    const budgets = option.maxCommentsByFile;

    if (typeof budgets === "object" && budgets !== null) {
      for (const [file, budget] of Object.entries(budgets)) {
        if (!existsSync(file)) {
          failures.push(`.oxlintrc.json no-comments ratchet names missing file ${file}`);
        }

        if (typeof budget !== "number" || budget <= 0) {
          failures.push(
            `.oxlintrc.json no-comments ratchet for ${file} must be a positive integer (remove entry when 0)`,
          );
        }
      }
    }
  }
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

console.log(
  `turbo lint rules: ${expected.size} expected diagnostics, ratchet verified, config matches RULE_OWNERS`,
);
