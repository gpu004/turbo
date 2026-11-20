// A route is declared once as a pattern such as "/queues/:queue/jobs/:jobId".
// The handler's `params` type is derived from that pattern, so a handler can
// only read parameters its route actually captures.

type ParamName<P extends string> = P extends `${infer Head}/${infer Tail}`
  ? ParamName<Head> | ParamName<Tail>
  : P extends `:${infer Name}`
    ? Name
    : never;

export type RouteParams<P extends string> = { readonly [K in ParamName<P>]: string };

export type Method = "GET" | "POST";

export interface Route<C> {
  readonly match: (
    method: string,
    segments: readonly string[],
  ) => ((context: C) => Promise<Response>) | undefined;
}

export function route<P extends string, C>(
  method: Method,
  pattern: P,
  handle: (context: C, params: RouteParams<P>) => Promise<Response>,
): Route<C> {
  const parts = pattern.split("/").filter(Boolean);

  return {
    match(requestMethod, segments) {
      if (requestMethod !== method || segments.length !== parts.length) return undefined;

      const captured = new Map<string, string>();

      for (const [index, part] of parts.entries()) {
        const segment = segments[index];

        if (segment === undefined) return undefined;

        if (part.startsWith(":")) captured.set(part.slice(1), segment);
        else if (part !== segment) return undefined;
      }

      // SAFETY: `captured` holds exactly one entry for each `:name` part of
      // `pattern`, and `ParamName<P>` is the set of those same names.
      const params = Object.fromEntries(captured) as RouteParams<P>;

      return (context) => handle(context, params);
    },
  };
}
