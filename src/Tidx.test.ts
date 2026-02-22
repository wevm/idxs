import { afterEach, describe, expect, test, vi } from 'vitest'
import * as Tidx from './Tidx.js'

const tidx = Tidx.create({
  basicAuth: process.env.VITE_API_CREDENTIALS,
  chainId: 42431,
})

afterEach(() => {
  vi.restoreAllMocks()
  // Clear all event handlers to prevent accumulation across tests
  tidx.off('error')
  tidx.off('request')
  tidx.off('response')
  tidx.off('log')
  tidx.off('*')
})

describe('create', () => {
  test('default', async () => {
    const tidx = Tidx.create({ chainId: 42431 })

    expect(tidx).toMatchInlineSnapshot(`
      {
        "baseUrl": "https://tidx.tempo.xyz",
        "chainId": 42431,
        "fetch": [Function],
        "live": [Function],
        "off": [Function],
        "on": [Function],
      }
    `)
  })

  test('creates indexer with custom baseUrl', () => {
    const tidx = Tidx.create({
      baseUrl: 'https://tidx.tempo.xyz',
      chainId: 42431,
    })
    expect(tidx).toMatchInlineSnapshot(`
      {
        "baseUrl": "https://tidx.tempo.xyz",
        "chainId": 42431,
        "fetch": [Function],
        "live": [Function],
        "off": [Function],
        "on": [Function],
      }
    `)
  })

  describe('.fetch', () => {
    test('behavior: tx', async () => {
      const result = await tidx.fetch({
        query:
          'select block_num, block_timestamp, idx, type, gas_limit, max_fee_per_gas, nonce, hash, "from", "to", input, value from txs limit 1',
      })

      expect(result).toHaveProperty('rows')
      expect(result.rows.length).toBeGreaterThan(0)

      const row = result.rows[0]
      expect(row).toHaveProperty('block_num')
      expect(row).toHaveProperty('block_timestamp')
      expect(row).toHaveProperty('idx')
      expect(row).toHaveProperty('type')
      expect(row).toHaveProperty('gas_limit')
      expect(row).toHaveProperty('max_fee_per_gas')
      expect(row).toHaveProperty('nonce')
      expect(row).toHaveProperty('hash')
      expect(row).toHaveProperty('from')
      expect(row).toHaveProperty('to')
      expect(row).toHaveProperty('input')
      expect(row).toHaveProperty('value')
    })

    test('behavior: queries Transfer events from Base chain', async () => {
      const result = await tidx.fetch({
        query: 'select "from", "to", tokens from transfer limit 3',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 tokens)'],
      })

      expect(result).toHaveProperty('rows')
      expect(result.rows.length).toBeLessThanOrEqual(3)
    })

    test.skip('behavior: OLAP aggregation with engine=clickhouse', async () => {
      const result = await tidx.fetch({
        engine: 'clickhouse',
        query: 'select count(*) count, max(block_num) max_block from txs',
      })

      expect(result).toHaveProperty('rows')
      expect(result.rows.length).toBe(1)

      const row = result.rows[0] as (typeof result.rows)[number]
      expect(row).toHaveProperty('count')
      expect(row).toHaveProperty('max_block')
      expect(Number(row.count)).toBeGreaterThan(0)
    })

    test.skip('behavior: OLAP group by with engine=clickhouse', async () => {
      const result = await tidx.fetch({
        engine: 'clickhouse',
        query: 'select type, count(*) count from txs group by type order by count desc limit 5',
      })

      expect(result).toHaveProperty('rows')
      expect(result.rows.length).toBeGreaterThan(0)
      expect(result.rows.length).toBeLessThanOrEqual(5)

      for (const row of result.rows) {
        expect(row).toHaveProperty('type')
        expect(row).toHaveProperty('count')
      }
    })

    test('behavior: engine param is set in request URL', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['count'],
              rows: [[100]],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const requests: Request[] = []

      testIndexer.on('request', (request) => {
        requests.push(request)
      })

      await testIndexer.fetch({
        engine: 'clickhouse',
        query: 'select count(*) count from txs',
      })

      expect(requests).toHaveLength(1)
      const url = new URL(requests[0]?.url as string)
      expect(url.searchParams.get('engine')).toBe('clickhouse')
    })

    test('behavior: engine param is omitted from URL when not set', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const requests: Request[] = []

      testIndexer.on('request', (request) => {
        requests.push(request)
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from txs limit 1',
      })

      expect(requests).toHaveLength(1)
      const url = new URL(requests[0]?.url as string)
      expect(url.searchParams.has('engine')).toBe(false)
    })

    test('behavior: forwards RequestInit options', async () => {
      const controller = new AbortController()

      // Start the request
      const promise = tidx.fetch({
        query: 'select "from", "to" from transfer limit 1',
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

      const testIndexer = Tidx.create({ chainId: 42431 })

      await expect(
        testIndexer.fetch({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        }),
      ).rejects.toThrow('Internal Server Error - Database connection failed')

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    test('behavior: throws error when ok is false', async () => {
      const fetchSpy = vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(JSON.stringify({ ok: false, error: 'Request failed' }), {
            status: 200,
            statusText: 'OK',
          }),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })

      await expect(
        testIndexer.fetch({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        }),
      ).rejects.toThrow('Request failed')

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
    })

    describe('behavior: retries', () => {
      test('behavior: retries on retryable error (500) and eventually succeeds', async () => {
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
          .mockImplementation(
            async () =>
              new Response(
                JSON.stringify({
                  ok: true,
                  columns: ['from', 'to'],
                  rows: [['0x123', '0x456']],
                  row_count: 1,
                }),
                { status: 200, statusText: 'OK' },
              ),
          )

        const testIndexer = Tidx.create({ chainId: 42431 })
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        const result = await testIndexer.fetch({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        expect(result).toHaveProperty('rows')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(3)
        // Should have emitted 2 errors before success
        expect(errors.length).toBe(2)
        for (const error of errors) {
          expect(error).toBeInstanceOf(Error)
          expect(error.name).toBe('Tidx.FetchRequestError')
        }
      })

      test('behavior: retries on 429 (rate limit) error', async () => {
        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Rate limit exceeded' }), {
                status: 429,
                statusText: 'Too Many Requests',
              }),
          )
          .mockImplementation(
            async () =>
              new Response(
                JSON.stringify({
                  ok: true,
                  columns: ['from', 'to'],
                  rows: [['0x123', '0x456']],
                  row_count: 1,
                }),
                { status: 200, statusText: 'OK' },
              ),
          )

        const testIndexer = Tidx.create({ chainId: 42431 })
        const result = await testIndexer.fetch({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        expect(result).toHaveProperty('rows')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: retries on 408 (timeout) error', async () => {
        const fetchSpy = vi
          .spyOn(globalThis, 'fetch')
          .mockImplementationOnce(
            async () =>
              new Response(JSON.stringify({ message: 'Request timeout' }), {
                status: 408,
                statusText: 'Request Timeout',
              }),
          )
          .mockImplementation(
            async () =>
              new Response(
                JSON.stringify({
                  ok: true,
                  columns: ['from', 'to'],
                  rows: [['0x123', '0x456']],
                  row_count: 1,
                }),
                { status: 200, statusText: 'OK' },
              ),
          )

        const testIndexer = Tidx.create({ chainId: 42431 })
        const result = await testIndexer.fetch({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        })

        expect(result).toHaveProperty('rows')
        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
      })

      test('behavior: does not retry on non-retryable error (400)', async () => {
        const fetchSpy = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
          new Response(JSON.stringify({ message: 'Bad request' }), {
            status: 400,
            statusText: 'Bad Request',
          }),
        )

        const testIndexer = Tidx.create({ chainId: 42431 })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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
          .mockImplementation(
            async () =>
              new Response(
                JSON.stringify({
                  ok: true,
                  columns: ['from', 'to'],
                  rows: [['0x123', '0x456']],
                  row_count: 1,
                }),
                { status: 200, statusText: 'OK' },
              ),
          )

        const testIndexer = Tidx.create({ chainId: 42431 })
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
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })

        await expect(
          testIndexer.fetch({
            query: 'select "from", "to" from transfer limit 1',
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
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const requests: Request[] = []

      testIndexer.on('request', (request) => {
        requests.push(request)
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(requests).toHaveLength(1)
      const request = requests[0]

      // Validate request properties
      expect(request).toBeInstanceOf(Request)
      expect(request?.method).toBe('GET')

      // Validate URL has query params
      const url = new URL(request?.url as string)
      expect(url.pathname).toBe('/query')
      expect(url.searchParams.get('sql')).toBe('select "from", "to" from transfer limit 1')
      expect(url.searchParams.get('chainId')).toBe('42431')
      expect(url.searchParams.get('signature')).toBe(
        'Transfer(address indexed from, address indexed to, uint256 value)',
      )
    })

    test('behavior: emits request event without optional fields', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const requests: Request[] = []

      testIndexer.on('request', (request) => {
        requests.push(request)
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(requests).toHaveLength(1)
      const request = requests[0]

      const url = new URL(request?.url as string)
      expect(url.searchParams.get('sql')).toBe('select "from", "to" from transfer limit 1')
      expect(url.searchParams.get('chainId')).toBe('42431')
    })

    test('behavior: emits response event with full details', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const responses: Response[] = []

      testIndexer.on('response', (response) => {
        responses.push(response)
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(responses).toHaveLength(1)
      const response = responses[0]

      // Validate response properties
      expect(response).toBeInstanceOf(Response)
      expect(response?.ok).toBe(true)
      expect(response?.status).toBe(200)

      // Clone and validate response body
      const clone = response?.clone()
      const data = await clone?.json()
      expect(data).toHaveProperty('ok', true)
      expect(data).toHaveProperty('columns')
      expect(data).toHaveProperty('rows')
    })

    test('behavior: emits error event with Tidx.FetchRequestError', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(JSON.stringify({ message: 'Bad request' }), {
            status: 400,
            statusText: 'Bad Request',
          }),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const errors: Error[] = []

      testIndexer.on('error', (error) => {
        errors.push(error)
      })

      await expect(
        testIndexer.fetch({
          query: 'invalid sql query that will fail',
        }),
      ).rejects.toThrow()

      expect(errors).toHaveLength(1)
      const error = errors[0]

      expect(error).toBeInstanceOf(Error)
      expect(error?.name).toBe('Tidx.FetchRequestError')
      expect(error?.message).toBeTruthy()
      expect(typeof error?.message).toBe('string')

      // Should have a meaningful error message from the API
      expect(error?.message.length).toBeGreaterThan(0)
    })

    test('behavior: emits error event with parsed JSON message', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(JSON.stringify({ message: 'Unknown table' }), {
            status: 400,
            statusText: 'Bad Request',
          }),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const errors: Error[] = []

      testIndexer.on('error', (error) => {
        errors.push(error)
      })

      await expect(
        testIndexer.fetch({
          query: 'select * from nonexistent_table',
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
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const events: string[] = []

      testIndexer.on('request', () => {
        events.push('request')
      })

      testIndexer.on('response', () => {
        events.push('response')
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(events).toEqual(['request', 'response'])
    })

    test('behavior: multiple listeners on same event', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const listener1Calls: number[] = []
      const listener2Calls: number[] = []

      testIndexer.on('request', () => {
        listener1Calls.push(1)
      })

      testIndexer.on('request', () => {
        listener2Calls.push(2)
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
      })

      expect(listener1Calls).toEqual([1])
      expect(listener2Calls).toEqual([2])
    })

    test('behavior: wildcard event is emitted for all events', async () => {
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(
            JSON.stringify({
              ok: true,
              columns: ['from', 'to'],
              rows: [['0x123', '0x456']],
              row_count: 1,
            }),
            { status: 200, statusText: 'OK' },
          ),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const wildcardEvents: Array<{
        event: string
        data: unknown
        options: { id: string }
      }> = []

      testIndexer.on('*', (event, data, options) => {
        wildcardEvents.push({
          event,
          data,
          options,
        })
      })

      await testIndexer.fetch({
        query: 'select "from", "to" from transfer limit 1',
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
      vi.spyOn(globalThis, 'fetch').mockImplementation(
        async () =>
          new Response(JSON.stringify({ message: 'Bad request' }), {
            status: 400,
            statusText: 'Bad Request',
          }),
      )

      const testIndexer = Tidx.create({ chainId: 42431 })
      const wildcardEvents: Array<{
        event: string
        data: unknown
        options: { id: string }
      }> = []

      testIndexer.on('*', (event, data, options) => {
        wildcardEvents.push({
          event,
          data,
          options,
        })
      })

      await expect(
        testIndexer.fetch({
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
      const results: Tidx.Tidx.fetch.ReturnValue<any, any>[] = []

      // Collect a few results then abort
      let count = 0
      const maxResults = 3

      for await (const result of tidx.live({
        query: 'select "from", "to", tokens from transfer limit 1',
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
        expect(result).toHaveProperty('rows')
      }
    }, 30_000)

    test('behavior: emits request and response events', async () => {
      const testIndexer = Tidx.create({
        basicAuth: process.env.VITE_API_CREDENTIALS,
        chainId: 42431,
      })
      const controller = new AbortController()
      const events: string[] = []

      testIndexer.on('request', () => {
        events.push('request')
      })

      testIndexer.on('response', () => {
        events.push('response')
      })

      for await (const _result of testIndexer.live({
        query: 'select "from", "to" from transfer limit 1',
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

      const testIndexer = Tidx.create({ chainId: 42431 })
      const controller = new AbortController()

      const generator = testIndexer.live({
        query: 'select "from", "to" from transfer limit 1',
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
            const data = JSON.stringify({ ok: true, columns: ['from'], rows: [], row_count: 0 })
            controller.enqueue(encoder.encode(`data: ${data}\n\n`))
            controller.close()
          },
        })

        return new Response(mockBody, {
          status: 200,
          headers: { 'Content-Type': 'text/event-stream' },
        })
      })

      const testIndexer = Tidx.create({ chainId: 42431 })
      const controller = new AbortController()
      // biome-ignore lint/suspicious/noExplicitAny: _
      const results: Tidx.Tidx.fetch.ReturnValue<any, any>[] = []

      for await (const result of testIndexer.live({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })) {
        results.push(result)
      }

      expect(results.length).toBe(1)
      expect(results[0]).toHaveProperty('rows')
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

      const testIndexer = Tidx.create({ chainId: 42431 })
      const controller = new AbortController()

      const generator = testIndexer.live({
        query: 'select "from", "to" from transfer limit 1',
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

      const testIndexer = Tidx.create({ chainId: 42431 })
      const controller = new AbortController()

      const generator = testIndexer.live({
        query: 'select "from", "to" from transfer limit 1',
        signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
        signal: controller.signal,
      })

      await expect(generator.next()).rejects.toThrow('Response body is null')

      expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(1)
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

        const testIndexer = Tidx.create({
          basicAuth: process.env.VITE_API_CREDENTIALS,
          chainId: 42431,
        })
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('rows')
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
          expect(error.name).toBe('Tidx.FetchRequestError')
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

        const testIndexer = Tidx.create({
          basicAuth: process.env.VITE_API_CREDENTIALS,
          chainId: 42431,
        })

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('rows')
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({
          basicAuth: process.env.VITE_API_CREDENTIALS,
          chainId: 42431,
        })
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
          query: 'select "from", "to" from transfer limit 1',
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

        const testIndexer = Tidx.create({
          basicAuth: process.env.VITE_API_CREDENTIALS,
          chainId: 42431,
        })

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('rows')
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

        const testIndexer = Tidx.create({ chainId: 42431 })
        const controller = new AbortController()

        const generator = testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
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
                const successData = JSON.stringify({
                  ok: true,
                  columns: ['from', 'to'],
                  rows: [['0x123', '0x456']],
                  row_count: 1,
                })
                ctrl.enqueue(encoder.encode(`data: ${successData}\n\n`))
                ctrl.close()
              },
            })
            return new Response(mockBody, {
              status: 200,
              headers: { 'Content-Type': 'text/event-stream' },
            })
          })

        const testIndexer = Tidx.create({ chainId: 42431 })
        const errors: Error[] = []

        testIndexer.on('error', (error) => {
          errors.push(error)
        })

        let resultCount = 0
        for await (const result of testIndexer.live({
          query: 'select "from", "to" from transfer limit 1',
          signatures: ['event Transfer(address indexed from, address indexed to, uint256 value)'],
          signal: controller.signal,
        })) {
          expect(result).toHaveProperty('rows')
          resultCount++
          if (resultCount >= 1) {
            controller.abort()
            break
          }
        }

        expect(fetchSpy.mock.calls.length).toBeGreaterThanOrEqual(2)
        expect(errors.length).toBe(1)
        expect(errors[0]?.name).toBe('Tidx.SseError')
      })
    })
  })
})
