// banned line comment // expect: turbo/no-comments
/* banned block comment */ // expect: turbo/no-comments
/** banned jsdoc comment */ // expect: turbo/no-comments
// eslint-disable-next-line // expect: turbo/no-comments

// @ts-expect-error allowed compiler directive
export const expectedError: number = "not a number";

// SAFETY: allowed single-line safety justification
export const singleSafety = "ok" as unknown as string;

/* SAFETY: allowed block safety justification */
export const blockSafety = "ok" as unknown as string;
