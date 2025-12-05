export type Compute<type> = { [key in keyof type]: type[key] } & unknown

/**
 * Checks if `T` is `never`
 *
 * @example
 * ```ts
 * type Result = IsNever<never>
 * //   ^? type Result = true
 * ```
 */
export type IsNever<T> = [T] extends [never] ? true : false

/**
 * Checks if `T` can be narrowed further than `U`
 *
 * @example
 * ```ts
 * type Result = IsNarrowable<'foo', string>
 * //   ^? true
 * ```
 */
export type IsNarrowable<T, U> =
  IsNever<(T extends U ? true : false) & (U extends T ? false : true)> extends true ? false : true

/** Recursively trim whitespace and newlines from both ends of a string. */
export type Trim<value extends string> = value extends ` ${infer Rest}`
  ? Trim<Rest>
  : value extends `${infer Rest} `
    ? Trim<Rest>
    : value extends `\n${infer Rest}`
      ? Trim<Rest>
      : value extends `${infer Rest}\n`
        ? Trim<Rest>
        : value extends `\t${infer Rest}`
          ? Trim<Rest>
          : value extends `${infer Rest}\t`
            ? Trim<Rest>
            : value

/** Remove surrounding quotes from an identifier (single or double quotes). */
export type Unquote<value extends string> = value extends `"${infer Name}"`
  ? Name
  : value extends `'${infer Name}'`
    ? Name
    : value

/** Find the last word in a string, handling spaces, newlines, and tabs as separators. */
export type LastWord<value extends string> = value extends `${infer _} ${infer Rest}`
  ? LastWord<Rest>
  : value extends `${infer _}\n${infer Rest}`
    ? LastWord<Rest>
    : value extends `${infer _}\t${infer Rest}`
      ? LastWord<Rest>
      : value

/** Whitespace character union type. */
export type Whitespace = ' ' | '\n' | '\t'

/** Case-insensitive string matching helper. */
export type CaseInsensitive<value extends string> = Uppercase<value> | Lowercase<value>
