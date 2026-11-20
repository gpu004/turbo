import { defineRule, eslintCompatPlugin } from "@oxlint/plugins";

import type { ESTree, SourceCode } from "@oxlint/plugins";

// Project rules for turbo. Each rule bans an operation everywhere, and
// `.oxlintrc.json` turns it off for the single module that owns it. That way
// the owning module is the one place an agent can add that kind of code.

// Runtime globals such as Workers' `crypto` are not in Oxlint's builtin env, so
// an identifier with no local binding also counts as the global.
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

/** Keep the clock and randomness in src/domain.ts so the model stays deterministic. */
const noAmbientEffectsRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description: "Disallow reading the clock or randomness outside the module that owns them.",
    },
    messages: {
      ambient:
        "`{{name}}` reads the clock or randomness. Use `currentInstant`, `addSeconds`, `newJobId`, or `newLeaseToken` from src/domain.ts, or accept the value as a parameter.",
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

/** Untrusted JSON text is parsed in one place and then validated by a schema. */
const noJsonParseRule = defineRule({
  meta: {
    type: "problem",
    docs: { description: "Disallow JSON.parse outside the JSON boundary modules." },
    messages: {
      jsonParse:
        "Use `parseJson` from src/domain.ts, then validate the value with a zod schema. For broker RPC values use `decode` from src/rpc.ts.",
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

/** Env vars are strings; read them through the parsed `Config` instead. */
const noRawEnvVarsRule = defineRule({
  meta: {
    type: "problem",
    docs: {
      description:
        "Disallow reading env vars directly; only resource bindings may be read from env.",
    },
    messages: {
      rawEnv:
        "`env.{{name}}` is an unparsed string. Add it to the schema in src/config.ts and read the typed `Config` field instead.",
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

// The only files `.oxlintrc.json` may turn each rule off for. `test.ts` fails
// if the config disables a rule anywhere else, so silencing a rule for a new
// file means changing this list, which a reviewer will see.
export const RULE_OWNERS: Readonly<Record<string, readonly string[]>> = {
  "no-ambient-effects": ["src/domain.ts"],
  "no-json-parse": ["src/domain.ts", "src/rpc.ts"],
  "no-raw-env-vars": ["src/config.ts"],
};

const turboPlugin = eslintCompatPlugin({
  meta: { name: "turbo" },
  rules: {
    "no-ambient-effects": noAmbientEffectsRule,
    "no-json-parse": noJsonParseRule,
    "no-raw-env-vars": noRawEnvVarsRule,
  },
});

export default turboPlugin;
