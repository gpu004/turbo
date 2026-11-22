declare const encodedValue: unique symbol;

export type Encoded<T> = string & { readonly [encodedValue]: T };

export function encode<T>(value: T): Encoded<T> {
  /* SAFETY: `encodedValue` is a type-only phantom field; at runtime an `Encoded<T>` is exactly the JSON text of a `T`. */
  return JSON.stringify(value) as Encoded<T>;
}

export function decode<T>(text: Encoded<T>): T {
  /* SAFETY: `encode<T>` is the only producer of `Encoded<T>`, and every type sent over the broker boundary is plain JSON data (branded ids are plain strings at runtime), so parsing the text yields the original `T`. */
  return JSON.parse(text) as T;
}
