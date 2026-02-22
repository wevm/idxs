import * as Errors from 'ox/Errors'
import * as Emitter from './internal/emitter.js'
import * as Result from './internal/result.js'

export type Tidx = {
  /** The base URL of the tidx API. */
  baseUrl: string
  /** The chain ID to query. */
  chainId: number | undefined
  /**
   * Fetches data from the tidx API.
   *
   * @example
   * ```ts
   * const result = await tidx.fetch({
   *   query: 'select hash, "from", "to", value from txs limit 10',
   * })
   * console.log(result.rows)
   * ```
   *
   * @example
   * ```ts
   * // With event signatures for custom tables
   * const result = await tidx.fetch({
   *   query: 'select "from", "to", value from transfer limit 10',
   *   signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
   * })
   * ```
   *
   * @param options - Options for the fetch method.
   * @returns The result of the operation.
   */
  fetch: <
    const query extends string = string,
    const signatures extends readonly Signature[] | undefined = undefined,
  >(
    options: Tidx.fetch.Options<query, signatures>,
  ) => Promise<Tidx.fetch.ReturnValue<query, signatures>>
  /**
   * Subscribes to live data updates from the tidx API via Server-Sent Events.
   *
   * @example
   * ```ts
   * for await (const result of tidx.live({
   *   chainId: 42431,
   *   query: 'select hash, "from", "to", value from txs limit 10',
   * }))
   *   console.log(result.rows)
   * ```
   *
   * @param options - Options for the live method.
   * @returns An iterable AsyncGenerator.
   */
  live: <
    query extends string = string,
    signatures extends readonly Signature[] | undefined = undefined,
  >(
    options: Tidx.live.Options<query, signatures>,
  ) => AsyncGenerator<Tidx.fetch.ReturnValue<query, signatures>, void, unknown>
  /**
   * Registers an event listener to listen for events from the tidx instance.
   *
   * @example
   * ```ts
   * tidx.on('*', (event, data) => console.log(event, data))
   * tidx.on('request', (request) => console.log('Request:', request.url))
   * tidx.on('response', (response) => console.log('Response:', response.status))
   * tidx.on('error', (error) => console.error('Error:', error.message))
   * ```
   */
  on: Emitter.Emitter['on']
  off: Emitter.Emitter['off']
}

/** Stringified signature of a function or event. */
export type Signature = Result.Signature

export declare namespace Tidx {
  export namespace fetch {
    /**
     * Options for the `fetch` method.
     */
    export type Options<
      query extends string = string,
      signatures extends readonly Signature[] | undefined = undefined,
    > = RequestInit & {
      /** Chain ID to query. Overrides the client-level `chainId` if set. */
      chainId?: number | undefined
      /** Query engine to use (e.g. `'clickhouse'` for OLAP queries). */
      engine?: string | undefined
      /** SQL query to execute. */
      query: query | string
      /** Optional number of retry attempts on failure. Defaults to 5. */
      retryCount?: number | undefined
      /** Optional array of event/function signatures for custom tables. */
      signatures?: signatures | readonly Signature[] | undefined
    }

    /**
     * Return value of the `fetch` method.
     */
    export type ReturnValue<
      query extends string = string,
      signatures extends readonly Signature[] | undefined = undefined,
    > = Result.Result<query, signatures>

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
      query extends string = string,
      signatures extends readonly Signature[] | undefined = undefined,
    > = fetch.Options<query, signatures>

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
 * Creates a tidx client instance.
 *
 * @param options - Configuration options for the client.
 * @returns A tidx client instance.
 *
 * @example
 * ```ts
 * import { Tidx } from 'tidx.ts'
 *
 * // Create with default options
 * const tidx = Tidx.create()
 *
 * // Create with API key
 * const tidx = Tidx.create({ basicAuth: 'your-api-key' })
 *
 * // Create with custom base URL
 * const tidx = Tidx.create({
 *   basicAuth: 'your-api-key',
 *   baseUrl: 'https://custom-api.example.com',
 * })
 * ```
 */
export function create(options: create.Options): create.ReturnValue {
  const { basicAuth, baseUrl = 'https://tidx.tempo.xyz', chainId } = options

  const emitter = Emitter.create()

  return {
    baseUrl,
    chainId,

    on: emitter.on.bind(emitter) as never,
    off: emitter.off.bind(emitter) as never,

    async fetch(options) {
      const {
        chainId: chainId_ = chainId,
        engine,
        signatures,
        query,
        retryCount = 5,
        ...requestInit
      } = options

      const { emit } = emitter.instance()

      const url = new URL(`${baseUrl}/query`)
      url.searchParams.set('sql', query)
      url.searchParams.set('chainId', String(chainId_))
      if (engine) url.searchParams.set('engine', engine)
      if (signatures) {
        for (const sig of signatures)
          url.searchParams.append('signature', sig.replace(/^(event|function)\s+/i, ''))
      }

      let count = 0
      let lastError: Error | undefined

      while (count < retryCount) {
        count++

        try {
          const request = new Request(url, {
            ...requestInit,
            method: 'GET',
            headers: {
              ...(basicAuth ? { Authorization: `Basic ${btoa(basicAuth)}` } : {}),
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

          const result = (await response.json()) as Result.Raw
          if (!result.ok) throw new FetchRequestError(result.error || 'Request failed', response)
          return Result.parse(result, { query, signatures }) as never
        } catch (e) {
          const error = e as Tidx.fetch.ErrorType

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
      const {
        chainId: chainId_ = chainId,
        engine,
        signatures,
        query,
        retryCount = 50,
        signal,
        ...requestInit
      } = options

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
          const url = new URL(`${baseUrl}/query`)
          url.searchParams.set('sql', query)
          url.searchParams.set('chainId', String(chainId_))
          url.searchParams.set('live', 'true')
          if (engine) url.searchParams.set('engine', engine)
          if (signatures) {
            for (const sig of signatures)
              url.searchParams.append('signature', sig.replace(/^(event|function)\s+/i, ''))
          }

          const request = new Request(url, {
            ...requestInit,
            ...(signal ? { signal } : {}),
            method: 'GET',
            headers: {
              ...(basicAuth ? { Authorization: `Basic ${btoa(basicAuth)}` } : {}),
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
          for await (const data of readStream<Result.Raw | Error>(reader)) {
            if (
              'error' in data &&
              typeof data.error === 'string' &&
              (data.error === 'client' || data.error === 'server')
            )
              throw new SseError((data as Error).message, { type: (data as Error).error })
            const raw = data as Result.Raw
            if (!raw.ok) throw new SseError(raw.error || 'Request failed', { type: 'server' })
            yield Result.parse(raw, { query, signatures }) as never
            count = 0
          }

          return
        } catch (e) {
          if (shouldAbort) return

          const error = e as Tidx.live.ErrorType

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
   * Options for creating a tidx client.
   */
  export type Options = {
    /** Basic auth credentials. */
    basicAuth?: string | undefined
    /** tidx API base URL. Defaults to `'https://tidx.tempo.xyz'`. */
    baseUrl?: string | undefined
    /** Chain ID to query. Can also be set per-call via `fetch`/`live` options. */
    chainId?: number | undefined
  }

  /**
   * Return type of the `create` function.
   */
  export type ReturnValue = Tidx
}

/**
 * Error thrown when a fetch request to the tidx API fails.
 *
 * @example
 * ```ts
 * try {
 *   await tidx.fetch({ chainId: 42431, query: 'invalid query' })
 * } catch (error) {
 *   if (error instanceof Tidx.FetchRequestError) {
 *     console.error('Request failed:', error.message)
 *     console.error('Status:', error.status)
 *   }
 * }
 * ```
 */
export class FetchRequestError extends Errors.BaseError {
  override readonly name = 'Tidx.FetchRequestError'

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
 *   for await (const result of tidx.live({ chainId: 42431, query: 'select * from txs' })) {
 *     console.log(result)
 *   }
 * } catch (error) {
 *   if (error instanceof Tidx.SseError) {
 *     console.error('SSE error:', error.message)
 *     console.error('Error type:', error.type) // 'client' or 'server'
 *   }
 * }
 * ```
 */
export class SseError extends Errors.BaseError<Error | undefined> {
  override readonly name = 'Tidx.SseError'

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
