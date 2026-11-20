export const value = JSON.parse("1"); // expect: turbo/no-json-parse
export const computed = JSON["parse"]("1"); // expect: turbo/no-json-parse

export const text = JSON.stringify(1);
