import * as Errors from 'ox/Errors'
import * as Emitter from './internal/emitter.js'
import * as Result from './internal/result.js'

export type IS = {
  /** The base URL of the Index Supply API. */
  baseUrl: string
  /**
   * Fetches data from the Index Supply API.
   *
   * @example
   * ```ts
   * const result = await is.fetch({
   *   query: 'select hash, "from", "to", value from txs limit 10',
   * })
   * console.log(result.rows)
   * ```
   *
   * @example
   * ```ts
   * // With event signatures for custom tables
   * const result = await is.fetch({
   *   query: 'select "from", "to", value from transfer limit 10',
   *   signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
   * })
   * ```
   *
   * @param options - Options for the fetch method.
   * @returns The result of the operation.
   */
  fetch: <
    const sql extends string = string,
    const signatures extends readonly Signature[] | undefined = undefined,
  >(
    options: IS.fetch.Options<sql, signatures>,
  ) => Promise<IS.fetch.ReturnValue<sql, signatures>>
  /**
   * Subscribes to live data updates from the Index Supply API via Server-Sent Events.
   *
   * @example
   * ```ts
   * for await (const result of is.live({
   *   query: 'select hash, "from", "to", value from txs limit 10',
   * }))
   *   console.log(result.rows)
   * ```
   *
   * @param options - Options for the live method.
   * @returns An iterable AsyncGenerator.
   */
  live: <
    sql extends string = string,
    signatures extends readonly Signature[] | undefined = undefined,
  >(
    options: IS.live.Options<sql, signatures>,
  ) => AsyncGenerator<IS.fetch.ReturnValue<sql, signatures>, void, unknown>
  /**
   * Registers an event listener to listen for events from the Index Supply instance.
   *
   * @example
   * ```ts
   * is.on('debug', (event, data) => console.log('Debug:', event, data))
   * is.on('request', (request) => console.log('Request:', request.url))
   * is.on('response', (response) => console.log('Response:', response.status))
   * is.on('error', (error) => console.error('Error:', error.message))
   * ```
   */
  on: Emitter.Emitter['on']
}

/** Cursor type for pagination. Can be a string or an object with `chainId` and `blockNumber`. */
export type Cursor = string | { chainId: number; blockNumber: number | bigint }

/** Stringified signature of a function or event. */
export type Signature = Result.Signature

export declare namespace IS {
  export namespace fetch {
    /**
     * Options for the `fetch` method.
     */
    export type Options<
      sql extends string = string,
      signatures extends readonly Signature[] | undefined = undefined,
    > = RequestInit & {
      /** Optional cursor for pagination. */
      cursor?: Cursor | undefined
      /** Optional number of retry attempts on failure. Defaults to 5. */
      retryCount?: number | undefined
      /** Optional array of event/function signatures for custom tables. */
      signatures?: signatures | readonly Signature[] | undefined
      /** SQL query to execute. */
      query: sql | string
    }

    /**
     * Return value of the `fetch` method.
     */
    export type ReturnValue<
      sql extends string = string,
      signatures extends readonly Signature[] | undefined = undefined,
    > = Result.Result<sql, signatures>

    /**
     * Error types that can be thrown by the `fetch` method.
     */
    export type ErrorType = FetchRequestError | Errors.GlobalErrorType
  }

  export namespace live {
    /**
     * Options for the `live` method.
     */
    export type Options<
      sql extends string = string,
      signatures extends readonly Signature[] | undefined = undefined,
    > = fetch.Options<sql, signatures>

    /**
     * Error types that can be thrown by the `live` method.
     */
    export type ErrorType = FetchRequestError | SseError | Errors.GlobalErrorType
  }

  /**
   * Result type returned by fetch and live methods.
   */
  export type Result = Result.Result
}

/**
 * Creates an Index Supply client instance.
 *
 * @param options - Configuration options for the client.
 * @returns An Index Supply client instance.
 *
 * @example
 * ```ts
 * import { IndexSupply } from 'idxs'
 *
 * // Create with default options
 * const is = IndexSupply.create()
 *
 * // Create with API key
 * const is = IndexSupply.create({ apiKey: 'your-api-key' })
 *
 * // Create with custom base URL
 * const is = IndexSupply.create({
 *   apiKey: 'your-api-key',
 *   baseUrl: 'https://custom-api.example.com',
 * })
 * ```
 */
export function create(options: create.Options = {}): create.ReturnValue {
  const { apiKey, baseUrl = 'https://api.indexsupply.net/v2' } = options

  const emitter = Emitter.create()

  return {
    baseUrl,

    on: emitter.on.bind(emitter) as never,

    async fetch(options) {
      const { cursor, signatures, query, retryCount = 5, ...requestInit } = options

      const { emit } = emitter.instance()

      const url = new URL(`${baseUrl}/query`)

      const requestBody: { cursor?: string; query: string; signatures?: string[] } = { query }
      if (cursor !== undefined)
        requestBody.cursor =
          typeof cursor === 'string' ? cursor : `${cursor.chainId}-${cursor.blockNumber.toString()}`
      if (signatures !== undefined) requestBody.signatures = signatures as string[]

      let count = 0
      let lastError: Error | undefined

      while (count < retryCount) {
        count++

        try {
          const request = new Request(url, {
            ...requestInit,
            body: JSON.stringify([requestBody]),
            headers: {
              'Content-Type': 'application/json',
              ...(apiKey ? { 'Api-Key': apiKey } : {}),
            },
            method: 'POST',
          })

          emit('request', request.clone())

          const response = await fetch(request)

          emit('response', response.clone())

          if (!response.ok) {
            const raw = await response.text()
            const message = (() => {
              try {
                return JSON.parse(raw).message
              } catch {
                return raw
              }
            })()
            throw new FetchRequestError(message, response)
          }

          const [result] = (await response.json()) as [Result.Raw]
          if (!result) throw new Errors.BaseError('No results returned')
          return Result.parse(result, { query, signatures }) as never
        } catch (e) {
          const error = e as IS.fetch.ErrorType

          emit('error', error)

          if (!shouldRetry(error)) throw error

          lastError = error

          await new Promise((resolve) =>
            setTimeout(resolve, Math.min(200 * 2 ** (count - 1), 30_000)),
          )
        }
      }

      lastError ??= new Errors.BaseError('Maximum retry attempts reached')

      throw lastError
    },

    async *live(options) {
      const { cursor, signatures, query, retryCount = 50, signal, ...requestInit } = options

      const { emit } = emitter.instance()

      let shouldAbort = false
      signal?.addEventListener('abort', () => {
        shouldAbort = true
      })

      let count = 0
      let lastError: Error | undefined

      while (count < retryCount) {
        count++

        try {
          const url = new URL(`${baseUrl}/query-live`)

          const params = new URLSearchParams()
          if (cursor)
            params.set(
              'cursor',
              typeof cursor === 'string'
                ? cursor
                : `${cursor.chainId}-${cursor.blockNumber.toString()}`,
            )
          if (signatures) for (const sig of signatures) params.append('signatures', sig)
          params.set('query', query)

          url.search = params.toString()

          const request = new Request(url, {
            ...requestInit,
            ...(signal ? { signal } : {}),
            method: 'GET',
            headers: {
              ...(apiKey ? { 'Api-Key': apiKey } : {}),
            },
          })

          emit('request', request.clone())

          const response = await fetch(request)

          emit('response', response.clone())

          if (!response.ok) {
            const raw = await response.text()
            const message = (() => {
              try {
                return JSON.parse(raw).message
              } catch {
                return raw
              }
            })()
            throw new FetchRequestError(message, response)
          }
          if (!response.body) throw new Errors.BaseError('Response body is null')

          const reader = response.body.getReader()

          type Error =
            | {
                error: 'client'
                message: string
              }
            | {
                error: 'server'
                message: string
              }
          for await (const data of readStream<IS.Result[] | Error>(reader)) {
            if ('error' in data) throw new SseError(data.message, { type: data.error })
            for (const item of data) yield item as never
            count = 0
          }

          return
        } catch (e) {
          if (shouldAbort) return

          const error = e as IS.live.ErrorType

          emit('error', error)

          if (!shouldRetry(error)) throw error

          lastError = error

          await new Promise((resolve) =>
            setTimeout(resolve, Math.min(200 * 2 ** (count - 1), 30_000)),
          )
        }
      }

      lastError ??= new Errors.BaseError('Maximum retry attempts reached')

      throw lastError
    },
  }
}

export declare namespace create {
  /**
   * Options for creating an Index Supply client.
   */
  export type Options = {
    /** Index Supply API key for authentication. */
    apiKey?: string | undefined
    /** Index Supply API base URL. Defaults to `'https://api.indexsupply.net/v2'`. */
    baseUrl?: string | undefined
  }

  /**
   * Return type of the `create` function.
   */
  export type ReturnValue = IS
}

/**
 * Error thrown when a fetch request to the Index Supply API fails.
 *
 * @example
 * ```ts
 * try {
 *   await is.fetch({ query: 'invalid query' })
 * } catch (error) {
 *   if (error instanceof IndexSupply.FetchRequestError) {
 *     console.error('Request failed:', error.message)
 *     console.error('Status:', error.status)
 *   }
 * }
 * ```
 */
export class FetchRequestError extends Errors.BaseError {
  override readonly name = 'IndexSupply.FetchRequestError'

  /** The HTTP response object. */
  response: Response
  /** The HTTP status code. */
  status: number

  /**
   * Creates a new FetchRequestError.
   *
   * @param message - The error message.
   * @param response - The HTTP response object.
   */
  constructor(message: string, response: Response) {
    super(message, {
      metaMessages: [`Status: ${response.status}`],
    })

    this.response = response
    this.status = response.status
  }
}

/**
 * Error thrown when a Server-Sent Events (SSE) connection fails.
 *
 * @example
 * ```ts
 * try {
 *   for await (const result of is.live({ query: 'select * from txs' })) {
 *     console.log(result)
 *   }
 * } catch (error) {
 *   if (error instanceof IndexSupply.SseError) {
 *     console.error('SSE error:', error.message)
 *     console.error('Error type:', error.type) // 'client' or 'server'
 *   }
 * }
 * ```
 */
export class SseError extends Errors.BaseError<Error | undefined> {
  override readonly name = 'IndexSupply.SseError'

  /** The type of error: 'client' for client-side errors, 'server' for server-side errors. */
  type: 'client' | 'server'

  /**
   * Creates a new SseError.
   *
   * @param message - The error message.
   * @param options - Error options including cause and type.
   */
  constructor(
    message: string,
    options: {
      cause?: Error | undefined
      type: 'client' | 'server'
    },
  ) {
    const { cause, type } = options
    super(message, { cause })

    this.type = type
  }
}

/** @internal */
async function* readStream<result>(
  reader: ReadableStreamDefaultReader<Uint8Array>,
): AsyncGenerator<result> {
  const decoder = new TextDecoder('utf-8')
  let buffer = ''

  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) break

      const decoded = decoder.decode(value, { stream: true })
      buffer += decoded

      let idx = buffer.indexOf('\n\n')
      while (idx !== -1) {
        const block = buffer.slice(0, idx)
        buffer = buffer.slice(idx + 2)

        const lines = block.split('\n')
        for (const line of lines) {
          if (line.startsWith('data:')) {
            const json = line.slice(5).trim()
            try {
              yield JSON.parse(json)
            } catch (e) {
              const error = e as Error
              await reader.cancel('Invalid JSON in data line')
              throw new SseError(error.message, { cause: error, type: 'client' })
            }
          }
        }

        idx = buffer.indexOf('\n\n')
      }
    }
  } finally {
    await reader.cancel('Stream closed')
  }
}

/** @internal */
function shouldRetry(error: Error): boolean {
  if (error instanceof SseError && error.type === 'server') return true
  if (
    error instanceof FetchRequestError &&
    (error.status === 408 || error.status === 429 || error.status >= 500)
  )
    return true
  return false
}
