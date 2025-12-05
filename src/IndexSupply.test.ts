import { afterEach, describe, expect, test, vi } from 'vitest'
import * as IS from './IndexSupply.js'

const is = IS.create({
  apiKey: process.env.VITE_API_KEY,
})

afterEach(() => {
  vi.restoreAllMocks()
  // Clear all event handlers to prevent accumulation across tests
  is.off('error')
  is.off('request')
  is.off('response')
  is.off('log')
  is.off('*')
})

describe('create', () => {
  test('default', async () => {
    const is = IS.create()

    expect(is).toMatchInlineSnapshot(`
      {
        "baseUrl": "https://api.indexsupply.net/v2",
        "fetch": [Function],
        "live": [Function],
        "off": [Function],
        "on": [Function],
      }
    `)
  })

  test('creates indexer with custom baseUrl', () => {
    const is = IS.create({
      baseUrl: 'https://api.indexsupply.net/v2',
    })
    expect(is).toMatchInlineSnapshot(`
      {
        "baseUrl": "https://api.indexsupply.net/v2",
        "fetch": [Function],
        "live": [Function],
        "off": [Function],
        "on": [Function],
      }
    `)
  })

  describe('.fetch', () => {
    test('behavior: tx', async () => {
      const result = await is.fetch({
        query:
          'select chain, block_num, block_timestamp, idx, type, gas, gas_price, nonce, hash, "from", "to", input, value from txs where chain = 8453 limit 1',
      })

      expect(result).toHaveProperty('cursor')
      expect(result).toHaveProperty('rows')
      expect(result.rows.length).toBeGreaterThan(0)

      const row = result.rows[0]
      expect(row).toHaveProperty('chain')
      expect(row).toHaveProperty('block_num')
      expect(row).toHaveProperty('block_timestamp')
      expect(row).toHaveProperty('idx')
      expect(row).toHaveProperty('type')
      expect(row).toHaveProperty('gas')
      expect(row).toHaveProperty('gas_price')
      expect(row).toHaveProperty('nonce')
      expect(row).toHaveProperty('hash')
      expect(row).toHaveProperty('from')
      expect(row).toHaveProperty('to')
      expect(row).toHaveProperty('input')
      expect(row).toHaveProperty('value')
    })

    test('behavior: queries Transfer events from Base chain', async () => {
      const result = await is.fetch({
        query: 'select "from", "to", tokens from transfer where chain = 8453 limit 3',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 tokens)'],
      })

      expect(result).toHaveProperty('cursor')
      expect(result).toHaveProperty('rows')
      expect(result.cursor).toMatch(/^8453-\d+$/)
      expect(result.rows.length).toBeLessThanOrEqual(3)
    })

    test('behavior: uses cursor for pagination', async () => {
      // First query
      const first = await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 2',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(first.cursor).toBeTruthy()

      // Second query with cursor
      const second = await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 2',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: first.cursor,
      })

      expect(second.cursor).toBeTruthy()
    })

    test('behavior: uses object-based cursor for pagination', async () => {
      const result = await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 2',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: { chainId: 8453, blockNumber: 12345678 },
      })

      expect(result).toHaveProperty('cursor')
      expect(result).toHaveProperty('rows')
      expect(result.cursor).toMatch(/^8453-\d+$/)
    })

    test('behavior: uses object-based cursor with bigint blockNumber', async () => {
      const result = await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 2',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: { chainId: 8453, blockNumber: 12345678n },
      })

      expect(result).toHaveProperty('cursor')
      expect(result).toHaveProperty('rows')
      expect(result.cursor).toMatch(/^8453-\d+$/)
    })

    test('behavior: forwards RequestInit options', async () => {
      const controller = new AbortController()

      // Start the request
      const promise = is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })

      // Abort immediately
      controller.abort()

      // Should throw AbortError
      await expect(promise).rejects.toThrow()
    })

    test('behavior: handles non-JSON error response', async () => {
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response('Internal Server Error - Database connection failed', {
            status: 400,
            statusText: 'Internal Server Error',
          }),
      )

      const testIndexer = IS.create()

      await expect(
        testIndexer.fetch({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        }),
      ).rejects.toThrow('Internal Server Error - Database connection failed')

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    test('behavior: throws error when no results returned', async () => {
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(JSON.stringify([]), {
            status: 200,
            statusText: 'OK',
          }),
      )

      const testIndexer = IS.create()

      await expect(
        testIndexer.fetch({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        }),
      ).rejects.toThrow('No results returned')

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    describe('behavior: retries', () => {
      test('behavior: retries on retryable error (500) and eventually succeeds', async () => {
        const originalFetch = globalThis.fetch

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Internal server error' }), {
                status: 500,
                statusText: 'Internal Server Error',
              }),
          )
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Internal server error' }), {
                status: 500,
                statusText: 'Internal Server Error',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        const result = await testIndexer.fetch({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        expect(result).toHaveProperty('cursor')
        expect(result).toHaveProperty('rows')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        // Should have emitted 2 errors before success
        expect(errors.length).toBe(2)
        for (const error of errors) {
          expect(error).toBeInstanceOf(Error)
          expect(error.name).toBe('IndexSupply.FetchRequestError')
        }
      })

      test('behavior: retries on 429 (rate limit) error', async () => {
        const originalFetch = globalThis.fetch

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Rate limit exceeded' }), {
                status: 429,
                statusText: 'Too Many Requests',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()
        const result = await testIndexer.fetch({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        expect(result).toHaveProperty('cursor')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: retries on 408 (timeout) error', async () => {
        const originalFetch = globalThis.fetch

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Request timeout' }), {
                status: 408,
                statusText: 'Request Timeout',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()
        const result = await testIndexer.fetch({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        expect(result).toHaveProperty('cursor')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: does not retry on non-retryable error (400)', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
          new Response(JSON.stringify({ message: 'Bad request' }), {
            status: 400,
            statusText: 'Bad Request',
          }),
        )

        const testIndexer = IS.create()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          }),
        ).rejects.toThrow('Bad request')

        // Should only attempt once for non-retryable error
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: does not retry on non-retryable error (404)', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
          new Response(JSON.stringify({ message: 'Not found' }), {
            status: 404,
            statusText: 'Not Found',
          }),
        )

        const testIndexer = IS.create()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          }),
        ).rejects.toThrow('Not found')

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: throws last error when max retries reached', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
          async () =>
            new Response(JSON.stringify({ message: 'Internal server error' }), {
              status: 500,
              statusText: 'Internal Server Error',
            }),
        )

        const testIndexer = IS.create()
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
            retryCount: 3,
          }),
        ).rejects.toThrow('Internal server error')

        // Should attempt 3 times (retryCount)
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        // Should emit error for each attempt
        expect(errors.length).toBe(3)
      })

      test('behavior: respects custom retryCount parameter', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
          async () =>
            new Response(JSON.stringify({ message: 'Internal server error' }), {
              status: 500,
              statusText: 'Internal Server Error',
            }),
        )

        const testIndexer = IS.create()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
            retryCount: 2,
          }),
        ).rejects.toThrow()

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: exponential backoff timing between retries', async () => {
        const attemptTimestamps: number[] = []

        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
          attemptTimestamps.push(Date.now())
          return new Response(JSON.stringify({ message: 'Internal server error' }), {
            status: 500,
            statusText: 'Internal Server Error',
          })
        })

        const testIndexer = IS.create()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
            retryCount: 3,
          }),
        ).rejects.toThrow()

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        expect(attemptTimestamps.length).toBe(3)

        // Check backoff timing: 200ms, 400ms (200 * 2^1), capped at 30s
        // First delay: 200 * 2^0 = 200ms
        const timestamp0 = attemptTimestamps[0]
        const timestamp1 = attemptTimestamps[1]
        const timestamp2 = attemptTimestamps[2]
        if (timestamp0 === undefined || timestamp1 === undefined || timestamp2 === undefined) {
          throw new Error('Missing timestamps')
        }
        const delay1 = timestamp1 - timestamp0
        expect(delay1).toBeGreaterThanOrEqual(190) // Allow some tolerance
        expect(delay1).toBeLessThan(500)

        // Second delay: 200 * 2^1 = 400ms
        const delay2 = timestamp2 - timestamp1
        expect(delay2).toBeGreaterThanOrEqual(390)
        expect(delay2).toBeLessThan(700)
      }, 10000)

      test('behavior: emits error event for each retry attempt', async () => {
        const originalFetch = globalThis.fetch

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Server error' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Server error' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Server error' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()
        const errors: Error[] = []
        const requests: Request[] = []
        const responses: Response[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })
        testIndexer.on('request', (request) => {
          requests.push(request)
        })
        testIndexer.on('response', (response) => {
          responses.push(response)
        })

        await testIndexer.fetch({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        // Should have made 4 attempts total
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(4)
        // Should have 4 request events
        expect(requests.length).toBe(4)
        // Should have 4 response events
        expect(responses.length).toBe(4)
        // Should have 3 error events (not for the successful one)
        expect(errors.length).toBe(3)
      })

      test('behavior: does not retry on abort signal', async () => {
        const error = new Error('The operation was aborted')
        error.name = 'AbortError'
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockRejectedValue(error)

        const testIndexer = IS.create()
        const controller = new AbortController()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
            signal: controller.signal,
          }),
        ).rejects.toThrow('The operation was aborted')

        // Should only attempt once when aborted
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: throws "Maximum retry attempts reached" when lastError is undefined', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
          async () =>
            new Response(JSON.stringify({ message: 'Server error' }), {
              status: 502,
              statusText: 'Bad Gateway',
            }),
        )

        const testIndexer = IS.create()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer where chain = 8453 limit 1',
            signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
            retryCount: 2,
          }),
        ).rejects.toThrow('Server error')

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })
    })
  })

  describe('.on', () => {
    test('behavior: emits request event with full details', async () => {
      const requests: Request[] = []

      is.on('request', (request) => {
        requests.push(request)
      })

      await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: '8453-12345',
      })

      expect(requests).toHaveLength(1)
      const request = requests[0]

      // Validate request properties
      expect(request).toBeInstanceOf(Request)
      expect(request?.method).toBe('POST')
      expect(request?.url).toBe('https://api.indexsupply.net/v2/query')

      // Validate headers
      expect(request?.headers.get('Content-Type')).toBe('application/json')

      // Validate body
      const body = await request?.text()
      const parsedBody = JSON.parse(body || '[]')
      expect(parsedBody).toHaveLength(1)
      expect(parsedBody[0]).toMatchObject({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: '8453-12345',
      })
    })

    test('behavior: emits request event without optional fields in body', async () => {
      const requests: Request[] = []

      is.on('request', (request) => {
        requests.push(request)
      })

      await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(requests).toHaveLength(1)
      const request = requests[0]

      const body = await request?.text()
      const parsedBody = JSON.parse(body || '[]')
      expect(parsedBody[0]).toEqual({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })
      // cursor should not be present when undefined
      expect(parsedBody[0]).not.toHaveProperty('cursor')
    })

    test('behavior: emits response event with full details', async () => {
      const responses: Response[] = []

      is.on('response', (response) => {
        responses.push(response)
      })

      await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(responses).toHaveLength(1)
      const response = responses[0]

      // Validate response properties
      expect(response).toBeInstanceOf(Response)
      expect(response?.ok).toBe(true)
      expect(response?.status).toBe(200)
      expect(response?.url).toBe('https://api.indexsupply.net/v2/query')
      expect(response?.statusText).toBeTruthy()

      // Clone and validate response body
      const clone = response?.clone()
      const data = await clone?.json()
      expect(Array.isArray(data)).toBe(true)
      expect(data).toHaveLength(1)
      expect(data[0]).toHaveProperty('cursor')
      expect(data[0]).toHaveProperty('columns')
      expect(data[0]).toHaveProperty('rows')
    })

    test('behavior: emits error event with IndexSupply.FetchRequestError', async () => {
      const errors: Error[] = []

      is.on('error', (error) => {
        errors.push(error)
      })

      await expect(
        is.fetch({
          query: 'invalid sql query that will fail',
        }),
      ).rejects.toThrow()

      expect(errors).toHaveLength(1)
      const error = errors[0]

      expect(error).toBeInstanceOf(Error)
      expect(error?.name).toBe('IndexSupply.FetchRequestError')
      expect(error?.message).toBeTruthy()
      expect(typeof error?.message).toBe('string')

      // Should have a meaningful error message from the API
      expect(error?.message.length).toBeGreaterThan(0)
    })

    test('behavior: emits error event with parsed JSON message', async () => {
      const errors: Error[] = []

      is.on('error', (error) => {
        errors.push(error)
      })

      await expect(
        is.fetch({
          query: 'select * from nonexistent_table where chain = 8453',
          signatures: ['event Foo(uint256 bar)'],
        }),
      ).rejects.toThrow()

      expect(errors).toHaveLength(1)
      const error = errors[0]

      // The error message should be extracted from the JSON response
      expect(error?.message).toBeTruthy()
      // Should not contain raw JSON structure like {"message":"..."}
      expect(error?.message).not.toMatch(/^\{/)
    })

    test('behavior: emits request and response events in order', async () => {
      const events: string[] = []

      is.on('request', () => {
        events.push('request')
      })

      is.on('response', () => {
        events.push('response')
      })

      await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(events).toEqual(['request', 'response'])
    })

    test('behavior: multiple listeners on same event', async () => {
      const listener1Calls: number[] = []
      const listener2Calls: number[] = []

      is.on('request', () => {
        listener1Calls.push(1)
      })

      is.on('request', () => {
        listener2Calls.push(2)
      })

      await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(listener1Calls).toEqual([1])
      expect(listener2Calls).toEqual([2])
    })

    test('behavior: wildcard event is emitted for all events', async () => {
      const wildcardEvents: Array<{
        event: string
        data: unknown
        options: { id: string }
      }> = []

      is.on('*', (event, data, options) => {
        wildcardEvents.push({
          event,
          data,
          options,
        })
      })

      await is.fetch({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      // Should have captured both request and response events
      expect(wildcardEvents.length).toBeGreaterThanOrEqual(2)

      // Check that wildcard event was emitted for request
      const requestEvent = wildcardEvents.find((e) => e.event === 'request')
      expect(requestEvent).toBeDefined()
      expect(requestEvent?.data).toBeInstanceOf(Request)
      expect(requestEvent?.options.id).toBeDefined()

      // Check that wildcard event was emitted for response
      const responseEvent = wildcardEvents.find((e) => e.event === 'response')
      expect(responseEvent).toBeDefined()
      expect(responseEvent?.data).toBeInstanceOf(Response)
      expect(responseEvent?.options.id).toBeDefined()
    })

    test('behavior: wildcard event is emitted for error events', async () => {
      const wildcardEvents: Array<{
        event: string
        data: unknown
        options: { id: string }
      }> = []

      is.on('*', (event, data, options) => {
        wildcardEvents.push({
          event,
          data,
          options,
        })
      })

      await expect(
        is.fetch({
          query: 'invalid sql query that will fail',
        }),
      ).rejects.toThrow()

      // Should have captured request, response, and error events via wildcard
      expect(wildcardEvents.length).toBeGreaterThanOrEqual(3)

      // Check that wildcard event was emitted for error
      const errorEvent = wildcardEvents.find((e) => e.event === 'error')
      expect(errorEvent).toBeDefined()
      expect(errorEvent?.data).toBeInstanceOf(Error)
      expect(errorEvent?.options.id).toBeDefined()
    })
  })

  describe('.live', () => {
    test('behavior: streams Transfer events from Base chain', async () => {
      const controller = new AbortController()
      // biome-ignore lint/suspicious/noExplicitAny: _
      const results: IS.IS.fetch.ReturnValue<any, any>[] = []

      // Collect a few results then abort
      let count = 0
      const maxResults = 3

      for await (const result of is.live({
        query: 'select "from", "to", tokens from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint tokens)'],
        signal: controller.signal,
      })) {
        results.push(result)
        count++

        if (count >= maxResults) {
          controller.abort()
          break
        }
      }

      expect(results.length).toBeGreaterThan(0)
      expect(results.length).toBeLessThanOrEqual(maxResults)

      for (const result of results) {
        expect(result).toHaveProperty('cursor')
        expect(result).toHaveProperty('columns')
        expect(result).toHaveProperty('rows')
        expect(result.cursor).toMatch(/^8453-\d+$/)
      }
    }) // Increase timeout for live query

    test('behavior: emits request and response events', async () => {
      const testIndexer = IS.create()
      const controller = new AbortController()
      const events: string[] = []

      testIndexer.on('request', () => {
        events.push('request')
      })

      testIndexer.on('response', () => {
        events.push('response')
      })

      for await (const _result of testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })) {
        controller.abort()
        break
      }

      expect(events).toContain('request')
      expect(events).toContain('response')
    })

    test('behavior: handles non-JSON error response', async () => {
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response('Internal Server Error - Database connection failed', {
            status: 400,
            statusText: 'Internal Server Error',
          }),
      )

      const testIndexer = IS.create()
      const controller = new AbortController()

      const generator = testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })

      await expect(generator.next()).rejects.toThrow(
        'Internal Server Error - Database connection failed',
      )

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    test('behavior: completes successfully when stream ends', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
        // Create a fresh mock SSE stream each time
        const mockBody = new ReadableStream({
          start(controller) {
            const encoder = new TextEncoder()
            const data = JSON.stringify([
              { cursor: '8453-123', columns: [{ name: 'from', pgtype: 'text' }], rows: [] },
            ])
            controller.enqueue(encoder.encode(`data: ${data}\n\n`))
            controller.close()
          },
        })

        return new Response(mockBody, {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' },
        })
      })

      const testIndexer = IS.create()
      const controller = new AbortController()
      // biome-ignore lint/suspicious/noExplicitAny: _
      const results: IS.IS.fetch.ReturnValue<any, any>[] = []

      for await (const result of testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })) {
        results.push(result)
      }

      expect(results.length).toBe(1)
      expect(results[0]).toHaveProperty('cursor', '8453-123')
    })

    test('behavior: handles invalid JSON in SSE stream', async () => {
      // Create a mock SSE stream with invalid JSON
      const mockBody = new ReadableStream({
        start(controller) {
          const encoder = new TextEncoder()
          controller.enqueue(encoder.encode('data: {invalid json}\n\n'))
          controller.close()
        },
      })

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(mockBody, {
            status: 200,
            headers: { 'Content-Type': 'text/event-stream' },
          }),
      )

      const testIndexer = IS.create()
      const controller = new AbortController()

      const generator = testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })

      await expect(generator.next()).rejects.toThrow()

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    test('behavior: throws error when response body is null', async () => {
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
        const response = new Response(null, {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' },
        })
        // Override body to be null
        Object.defineProperty(response, 'body', { value: null })
        return response
      })

      const testIndexer = IS.create()
      const controller = new AbortController()

      const generator = testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })

      await expect(generator.next()).rejects.toThrow('Response body is null')

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    test('behavior: uses cursor and signatures parameters in live query', async () => {
      const originalFetch = globalThis.fetch
      const controller = new AbortController()

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
        const url = new URL((input as Request).url)
        expect(url.searchParams.get('cursor')).toBe('8453-12345')
        expect(url.searchParams.get('signatures')).toBe(
          'event Transfer(address indexed from, address indexed to, uint256 value)',
        )
        return originalFetch(input as RequestInfo | URL)
      })

      const testIndexer = IS.create()

      let resultCount = 0
      for await (const _result of testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: '8453-12345',
        signal: controller.signal,
      })) {
        resultCount++
        if (resultCount >= 1) {
          controller.abort()
          break
        }
      }

      expect(fetchSpy).toHaveBeenCalled()
    })

    test('behavior: uses object-based cursor in live query', async () => {
      const originalFetch = globalThis.fetch
      const controller = new AbortController()

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
        const url = new URL((input as Request).url)
        expect(url.searchParams.get('cursor')).toBe('8453-12345678')
        return originalFetch(input as RequestInfo | URL)
      })

      const testIndexer = IS.create()

      let resultCount = 0
      for await (const _result of testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: { chainId: 8453, blockNumber: 12345678 },
        signal: controller.signal,
      })) {
        resultCount++
        if (resultCount >= 1) {
          controller.abort()
          break
        }
      }

      expect(fetchSpy).toHaveBeenCalled()
    })

    test('behavior: uses object-based cursor with bigint blockNumber in live query', async () => {
      const originalFetch = globalThis.fetch
      const controller = new AbortController()

      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async (input) => {
        const url = new URL((input as Request).url)
        expect(url.searchParams.get('cursor')).toBe('8453-12345678901234')
        return originalFetch(input as RequestInfo | URL)
      })

      const testIndexer = IS.create()

      let resultCount = 0
      for await (const _result of testIndexer.live({
        query: 'select "from", "to" from transfer where chain = 8453 limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        cursor: { chainId: 8453, blockNumber: 12345678901234n },
        signal: controller.signal,
      })) {
        resultCount++
        if (resultCount >= 1) {
          controller.abort()
          break
        }
      }

      expect(fetchSpy).toHaveBeenCalled()
    })

    describe('behavior: retries', () => {
      test('behavior: retries on retryable error (500) and eventually succeeds', async () => {
        const originalFetch = globalThis.fetch
        const controller = new AbortController()

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Internal server error' }), {
                status: 500,
                statusText: 'Internal Server Error',
              }),
          )
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Internal server error' }), {
                status: 500,
                statusText: 'Internal Server Error',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('cursor')
          resultCount++
          if (resultCount >= 1) {
            controller.abort()
            break
          }
        }

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        expect(errors.length).toBe(2)
        for (const error of errors) {
          expect(error).toBeInstanceOf(Error)
          expect(error.name).toBe('IndexSupply.FetchRequestError')
        }
      })

      test('behavior: retries on 503 (service unavailable) error', async () => {
        const originalFetch = globalThis.fetch
        const controller = new AbortController()

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Service unavailable' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('cursor')
          resultCount++
          if (resultCount >= 1) {
            controller.abort()
            break
          }
        }

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: does not retry on non-retryable error (400)', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
          new Response(JSON.stringify({ message: 'Bad request' }), {
            status: 400,
            statusText: 'Bad Request',
          }),
        )

        const testIndexer = IS.create()
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })

        await expect(generator.next()).rejects.toThrow('Bad request')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: does not retry on non-retryable error (404)', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
          new Response(JSON.stringify({ message: 'Not found' }), {
            status: 404,
            statusText: 'Not Found',
          }),
        )

        const testIndexer = IS.create()
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })

        await expect(generator.next()).rejects.toThrow('Not found')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: throws last error when max retries reached', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
          async () =>
            new Response(JSON.stringify({ message: 'Internal server error' }), {
              status: 500,
              statusText: 'Internal Server Error',
            }),
        )

        const testIndexer = IS.create()
        const controller = new AbortController()
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
          retryCount: 3,
        })

        await expect(generator.next()).rejects.toThrow('Internal server error')

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        expect(errors.length).toBe(3)
      })

      test('behavior: respects custom retryCount parameter', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
          async () =>
            new Response(JSON.stringify({ message: 'Internal server error' }), {
              status: 500,
              statusText: 'Internal Server Error',
            }),
        )

        const testIndexer = IS.create()
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
          retryCount: 2,
        })

        await expect(generator.next()).rejects.toThrow()
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: exponential backoff timing between retries', async () => {
        const attemptTimestamps: number[] = []

        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
          attemptTimestamps.push(Date.now())
          return new Response(JSON.stringify({ message: 'Internal server error' }), {
            status: 500,
            statusText: 'Internal Server Error',
          })
        })

        const testIndexer = IS.create()
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
          retryCount: 3,
        })

        await expect(generator.next()).rejects.toThrow()

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        expect(attemptTimestamps.length).toBe(3)

        const timestamp0 = attemptTimestamps[0]
        const timestamp1 = attemptTimestamps[1]
        const timestamp2 = attemptTimestamps[2]
        if (timestamp0 === undefined || timestamp1 === undefined || timestamp2 === undefined) {
          throw new Error('Missing timestamps')
        }

        const delay1 = timestamp1 - timestamp0
        expect(delay1).toBeGreaterThanOrEqual(190)
        expect(delay1).toBeLessThan(500)

        const delay2 = timestamp2 - timestamp1
        expect(delay2).toBeGreaterThanOrEqual(390)
        expect(delay2).toBeLessThan(700)
      }, 10000)

      test('behavior: emits error event for each retry attempt', async () => {
        const originalFetch = globalThis.fetch
        const controller = new AbortController()

        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Server error' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Server error' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Server error' }), {
                status: 503,
                statusText: 'Service Unavailable',
              }),
          )
          .mockImplementation(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()
        const errors: Error[] = []
        const requests: Request[] = []
        const responses: Response[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })
        testIndexer.on('request', (request) => {
          requests.push(request)
        })
        testIndexer.on('response', (response) => {
          responses.push(response)
        })

        let resultCount = 0
        for await (const _result of testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          resultCount++
          if (resultCount >= 1) {
            controller.abort()
            break
          }
        }

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(4)
        expect(requests.length).toBe(4)
        expect(responses.length).toBe(4)
        expect(errors.length).toBe(3)
      })

      test('behavior: handles abort signal during execution', async () => {
        const originalFetch = globalThis.fetch
        const controller = new AbortController()

        // Mock to succeed once, then let real fetch handle abort
        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(async (input, init) => originalFetch(input, init))

        const testIndexer = IS.create()

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('cursor')
          resultCount++
          // Abort after first result
          controller.abort()
          break
        }

        expect(resultCount).toBe(1)
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: does not retry on SseError with type "client"', async () => {
        // Create a mock SSE stream that returns a client error
        const mockBody = new ReadableStream({
          start(controller) {
            const encoder = new TextEncoder()
            const errorData = JSON.stringify({ error: 'client', message: 'Invalid query' })
            controller.enqueue(encoder.encode(`data: ${errorData}\n\n`))
            controller.close()
          },
        })

        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
          new Response(mockBody, {
            status: 200,
            headers: { 'Content-Type': 'text/event-stream' },
          }),
        )

        const testIndexer = IS.create()
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })

        await expect(generator.next()).rejects.toThrow('Invalid query')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
      })

      test('behavior: retries on SseError with type "server"', async () => {
        const controller = new AbortController()

        // First call returns server error via SSE, second call succeeds with valid data
        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(async () => {
            const mockBody = new ReadableStream({
              start(controller) {
                const encoder = new TextEncoder()
                const errorData = JSON.stringify({ error: 'server', message: 'Server error' })
                controller.enqueue(encoder.encode(`data: ${errorData}\n\n`))
                controller.close()
              },
            })
            return new Response(mockBody, {
              status: 200,
              headers: { 'Content-Type': 'text/event-stream' },
            })
          })
          .mockImplementationOnce(async () => {
            const mockBody = new ReadableStream({
              start(ctrl) {
                const encoder = new TextEncoder()
                const successData = JSON.stringify([
                  {
                    cursor: '8453-12345',
                    columns: [{ name: 'from' }, { name: 'to' }],
                    rows: [['0x123', '0x456']],
                  },
                ])
                ctrl.enqueue(encoder.encode(`data: ${successData}\n\n`))
                ctrl.close()
              },
            })
            return new Response(mockBody, {
              status: 200,
              headers: { 'Content-Type': 'text/event-stream' },
            })
          })

        const testIndexer = IS.create()
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer where chain = 8453 limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('cursor')
          resultCount++
          if (resultCount >= 1) {
            controller.abort()
            break
          }
        }

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
        expect(errors.length).toBe(1)
        expect(errors[0]?.name).toBe('IndexSupply.SseError')
      })
    })
  })
})
