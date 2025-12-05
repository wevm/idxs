import mitt from 'mitt'

export type Emitter = {
  instance: () => {
    emit: <K extends Events[number]['event']>(
      type: K,
      data: Extract<Events[number], { event: K }>['data'],
    ) => void
  }
  on: {
    <K extends Events[number]['event']>(
      type: K,
      handler: (
        data: Extract<Events[number], { event: K }>['data'],
        options: { id: string },
      ) => void,
    ): void
    (
      type: '*',
      handler: (
        type: Events[number]['event'],
        data: Events[number]['data'],
        options: { id: string },
      ) => void,
    ): void
  }
}

export type Events = [
  {
    data: Error
    event: 'error'
  },
  {
    data: string
    event: 'log'
  },
  {
    data: Request
    event: 'request'
  },
  {
    data: Response
    event: 'response'
  },
]

type InternalEvents = {
  [K in Events[number]['event']]: {
    data: Extract<Events[number], { event: K }>['data']
    id: string
  }
}

export function create(): Emitter {
  // @ts-expect-error -- this works
  const e = mitt<InternalEvents>()
  let id = -1

  function instance() {
    id++
    const instanceId = String(id)
    return {
      emit: ((type, data) => {
        // biome-ignore lint/suspicious/noExplicitAny: _
        e.emit(type, { data, id: instanceId } as any)
      }) satisfies Emitter['instance'] extends () => { emit: infer E } ? E : never,
    }
  }

  return {
    instance,
    // biome-ignore lint/suspicious/noExplicitAny: _
    on: ((type: string, handler: (...args: any[]) => void) => {
      if (type === '*') {
        // biome-ignore lint/suspicious/noExplicitAny: _
        e.on('*', (eventType: string, event: any) => {
          handler(eventType, event.data, { id: event.id })
        })
      } else {
        // biome-ignore lint/suspicious/noExplicitAny: _
        e.on(type as any, (event: any) => {
          handler(event.data, { id: event.id })
        })
      }
    }) as Emitter['on'],
  }
}
