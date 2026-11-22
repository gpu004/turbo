import { isAbsolute, relative } from "node:path";
import { defineRule, eslintCompatPlugin } from "@oxlint/plugins";

import type { ESTree, SourceCode } from "@oxlint/plugins";

function isGlobal(sourceCode: SourceCode, node: ESTree.Node, name: string): boolean {
  if (node.type !== "Identifier" || node.name !== name) return false;
  if (sourceCode.isGlobalReference(node)) return true;

  for (let scope = sourceCode.getScope(node); scope !== null; scope = scope.upper) {
    if (scope.set.get(name)?.defs.length) return false;
  }

  return true;
}

function propertyName(node: ESTree.MemberExpression): string | undefined {
  if (!node.computed && node.property.type === "Identifier") return node.property.name;
  if (
    node.computed &&
    node.property.type === "Literal" &&
    typeof node.property.value === "string"
  ) {
    return node.property.value;
  }

  return undefined;
}

const AMBIENT_MEMBERS: ReadonlyMap<string, ReadonlySet<string>> = new Map([
  ["Date", new Set(["now", "parse", "UTC"])],
  ["crypto", new Set(["randomUUID", "getRandomValues"])],
  ["Math", new Set(["random"])],
]);

const noAmbientEffectsRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description: "Disallow reading the clock or randomness outside the module that owns them.",
    },
    messages: {
      ambient:
        "`{{name}}` reads the clock or randomness. Use `currentInstant` or the `Ids` service from src/domain.ts, or accept the value as a parameter.",
    },
  },
  createOnce(context) {
    return {
      NewExpression(node) {
        if (isGlobal(context.sourceCode, node.callee, "Date")) {
          context.report({ node, messageId: "ambient", data: { name: "new Date" } });
        }
      },
      MemberExpression(node) {
        if (node.object.type !== "Identifier") return;

        const members = AMBIENT_MEMBERS.get(node.object.name);
        const name = propertyName(node);

        if (!members || !name || !members.has(name)) return;
        if (!isGlobal(context.sourceCode, node.object, node.object.name)) return;

        context.report({
          node,
          messageId: "ambient",
          data: { name: `${node.object.name}.${name}` },
        });
      },
    };
  },
});

const ALLOWED_DIRECTIVE =
  /^\s*(?:@ts-expect-error(?:\s|$)|SAFETY\s*:\s*\S|expect:\s*turbo\/[a-z-]+(?:\s|$))/u;

function isAllowedDirective(comment: ESTree.Comment): boolean {
  return comment.type === "Shebang" || ALLOWED_DIRECTIVE.test(comment.value);
}

function configuredMaxComments(option: unknown): ReadonlyMap<string, number> {
  if (typeof option !== "object" || option === null || !("maxCommentsByFile" in option)) {
    return new Map();
  }

  const raw = option.maxCommentsByFile;

  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return new Map();

  const limits = new Map<string, number>();

  for (const [file, limit] of Object.entries(raw)) {
    if (typeof limit === "number" && Number.isInteger(limit) && limit >= 0) {
      limits.set(file.replaceAll("\\", "/").replace(/^\.\//u, ""), limit);
    }
  }

  return limits;
}

function normalizeFilePath(cwd: string, filename: string): string {
  const path = isAbsolute(filename) ? relative(cwd, filename) : filename;

  return path.replaceAll("\\", "/").replace(/^\.\//u, "");
}

const noCommentsRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow prose comments, with an optional per-file ratchet budget for incremental cleanup.",
    },
    messages: {
      banned:
        "Comments are disallowed. Delete tautologies/stale comments, file an issue for excuses, or move decision records to docs/adr/. Run /delete-comment to process them.",
      exceedsBudget:
        "Comment exceeds this file's ratchet budget of {{budget}} ({{actual}} found). Delete comments instead of adding new ones, or run /delete-comment.",
      tightenRatchet:
        "This file now has {{actual}} comment(s), below its ratchet budget of {{budget}}. Lower maxCommentsByFile['{{file}}'] to {{actual}} (or remove the entry when 0).",
    },
    schema: [
      {
        type: "object",
        properties: {
          maxCommentsByFile: {
            type: "object",
            additionalProperties: { type: "integer", minimum: 0 },
          },
        },
        additionalProperties: false,
      },
    ],
  },
  createOnce(context) {
    return {
      Program(node) {
        const comments = context.sourceCode
          .getAllComments()
          .filter((comment) => !isAllowedDirective(comment));
        const maxByFile = configuredMaxComments(context.options?.[0]);
        const filePath = normalizeFilePath(context.cwd, context.filename);
        const budget = maxByFile.get(filePath);

        if (budget === undefined || budget <= 0) {
          for (const comment of comments) {
            context.report({ loc: comment.loc, messageId: "banned" });
          }

          return;
        }

        if (comments.length > budget) {
          for (const comment of comments.slice(budget)) {
            context.report({
              loc: comment.loc,
              messageId: "exceedsBudget",
              data: { budget, actual: comments.length },
            });
          }

          return;
        }

        if (comments.length < budget) {
          context.report({
            node,
            messageId: "tightenRatchet",
            data: { file: filePath, actual: comments.length, budget },
          });
        }
      },
    };
  },
});

const noJsonParseRule = defineRule({
  meta: {
    type: "problem",
    docs: { description: "Disallow JSON.parse outside the broker RPC boundary." },
    messages: {
      jsonParse:
        "Decode JSON text with `JsonText` from src/domain.ts, then decode the value with an Effect schema. For broker RPC values use `decode` from src/rpc.ts.",
    },
  },
  createOnce(context) {
    return {
      MemberExpression(node) {
        if (isGlobal(context.sourceCode, node.object, "JSON") && propertyName(node) === "parse") {
          context.report({ node, messageId: "jsonParse" });
        }
      },
    };
  },
});

function isEnvObject(node: ESTree.Node): boolean {
  if (node.type === "Identifier") return node.name === "env";

  return (
    node.type === "MemberExpression" &&
    node.object.type === "ThisExpression" &&
    !node.computed &&
    node.property.type === "Identifier" &&
    node.property.name === "env"
  );
}

function configuredBindings(option: unknown): ReadonlySet<string> {
  if (typeof option !== "object" || option === null || !("bindings" in option)) return new Set();
  if (!Array.isArray(option.bindings)) return new Set();

  return new Set(option.bindings.flatMap((value) => (typeof value === "string" ? [value] : [])));
}

const noRawEnvVarsRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow reading env vars directly; only resource bindings may be read from env.",
    },
    messages: {
      rawEnv:
        "`env.{{name}}` is an unparsed string. Add it to src/config.ts and read the typed field from the `Settings` service instead.",
    },
    schema: [
      {
        type: "object",
        properties: { bindings: { type: "array", items: { type: "string" } } },
        additionalProperties: false,
      },
    ],
  },
  createOnce(context) {
    return {
      MemberExpression(node) {
        if (!isEnvObject(node.object)) return;

        const name = propertyName(node);

        if (!name || configuredBindings(context.options?.[0]).has(name)) return;

        context.report({ node, messageId: "rawEnv", data: { name } });
      },
    };
  },
});

export const RULE_OWNERS: Readonly<Record<string, readonly string[]>> = {
  "no-ambient-effects": ["src/domain.ts"],
  "no-comments": [],
  "no-json-parse": ["src/rpc.ts"],
  "no-raw-env-vars": ["src/config.ts"],
};

const turboPlugin = eslintCompatPlugin({
  meta: { name: "turbo" },
  rules: {
    "no-ambient-effects": noAmbientEffectsRule,
    "no-comments": noCommentsRule,
    "no-json-parse": noJsonParseRule,
    "no-raw-env-vars": noRawEnvVarsRule,
  },
});

export default turboPlugin;
