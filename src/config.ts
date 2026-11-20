import { Config, ConfigProvider, Context, Layer, Schema, SchemaTransformation } from "effect";

// Env vars are parsed once into typed settings. Code outside this module reads
// the `Settings` service, never `env.*` strings. An out-of-range value falls
// back to its default, matching the documented behaviour of each variable.

function boundedInteger(name: SettingsVar, fallback: number, minimum: number, maximum: number) {
  const base10 = Schema.String.pipe(
    Schema.decodeTo(
      Schema.Number.check(Schema.isInt(), Schema.isBetween({ minimum, maximum })),
      SchemaTransformation.transform({
        decode: (value) => Number.parseInt(value, 10),
        encode: (value) => String(value),
      }),
    ),
  );

  return Config.schema(base10, name).pipe(Config.orElse(() => Config.succeed(fallback)));
}

type SettingsVar =
  | "QUEUE_BATCH_WINDOW_MS"
  | "QUEUE_LEASE_SECONDS"
  | "MAX_QUEUE_BYTES"
  | "MAX_REQUEST_BYTES"
  | "COMPLETED_RETENTION_SECONDS";

export type SettingsEnv = Partial<Readonly<Record<SettingsVar, string>>>;

const settingsConfig = Config.all({
  batchWindowMs: boundedInteger("QUEUE_BATCH_WINDOW_MS", 5, 0, 1_000),
  leaseSeconds: boundedInteger("QUEUE_LEASE_SECONDS", 60, 1, 86_400),
  maxQueueBytes: boundedInteger("MAX_QUEUE_BYTES", 8 * 1024 * 1024, 1_024, 32 * 1024 * 1024),
  maxRequestBytes: boundedInteger("MAX_REQUEST_BYTES", 256 * 1024, 1_024, 4 * 1024 * 1024),
  completedRetentionSeconds: boundedInteger("COMPLETED_RETENTION_SECONDS", 3_600, 1, 604_800),
});

export class Settings extends Context.Service<
  Settings,
  {
    readonly batchWindowMs: number;
    readonly leaseSeconds: number;
    readonly maxQueueBytes: number;
    readonly maxRequestBytes: number;
    readonly completedRetentionSeconds: number;
  }
>()("turbo/Settings") {
  // Every variable has a fallback, so parsing cannot fail.
  static fromEnv(env: SettingsEnv): Layer.Layer<Settings> {
    return Layer.effect(Settings, settingsConfig.parse(ConfigProvider.fromUnknown(env))).pipe(
      Layer.orDie,
    );
  }
}
