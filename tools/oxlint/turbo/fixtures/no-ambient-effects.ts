export const now = Date.now(); // expect: turbo/no-ambient-effects
export const later = new Date(); // expect: turbo/no-ambient-effects
export const parsed = Date["parse"]("2026-08-31"); // expect: turbo/no-ambient-effects
export const id = crypto.randomUUID(); // expect: turbo/no-ambient-effects
export const roll = Math.random(); // expect: turbo/no-ambient-effects

// Not the clock or randomness: a shadowing binding and other Math members.
export function shadowed(Date: { now(): number }): number {
  return Date.now();
}

export const larger = Math.max(1, 2);
