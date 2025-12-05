import mitt from 'mitt'

type EventHandler<K extends Events[number]['event']> = (
  data: Extract<Events[number], { event: K }>['data'],
  options: { id: string },
) => void

type WildcardHandler = (
  type: Events[number]['event'],
  data: Events[number]['data'],
  options: { id: string },
) => void

export type Emitter = {
  instance: () => {
    emit: <K extends Events[number]['event']>(
      type: K,
      data: Extract<Events[number], { event: K }>['data'],
    ) => void
  }
  on: {
    <K extends Events[number]['event']>(type: K, handler: EventHandler<K>): void
    (type: '*', handler: WildcardHandler): void
  }
  off: {
    <K extends Events[number]['event']>(type: K, handler?: EventHandler<K>): void
    (type: '*', handler?: WildcardHandler): void
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
  // biome-ignore lint/suspicious/noExplicitAny: _
  const wrappers = new WeakMap<(...args: any[]) => void, (...args: any[]) => void>()
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

  // biome-ignore lint/suspicious/noExplicitAny: _
  function on(type: string, handler: (...args: any[]) => void) {
    // biome-ignore lint/suspicious/noExplicitAny: _
    let wrapper: (...args: any[]) => void
    if (type === '*') {
      // biome-ignore lint/suspicious/noExplicitAny: _
      wrapper = (eventType: string, event: any) => {
        handler(eventType, event.data, { id: event.id })
      }
      e.on('*', wrapper)
    } else {
      // biome-ignore lint/suspicious/noExplicitAny: _
      wrapper = (event: any) => {
        handler(event.data, { id: event.id })
      }
      // biome-ignore lint/suspicious/noExplicitAny: _
      e.on(type as any, wrapper)
    }
    wrappers.set(handler, wrapper)
  }

  // biome-ignore lint/suspicious/noExplicitAny: _
  function off(type: string, handler?: (...args: any[]) => void) {
    if (!handler) {
      // Clear all handlers for this type
      if (type === '*') {
        e.all.delete('*')
      } else {
        // biome-ignore lint/suspicious/noExplicitAny: _
        e.all.delete(type as any)
      }
      return
    }
    const wrapper = wrappers.get(handler)
    if (!wrapper) return
    if (type === '*') {
      // biome-ignore lint/suspicious/noExplicitAny: _
      e.off('*', wrapper as any)
    } else {
      // biome-ignore lint/suspicious/noExplicitAny: _
      e.off(type as any, wrapper as any)
    }
    wrappers.delete(handler)
  }

  return {
    instance,
    on: on as Emitter['on'],
    off: off as Emitter['off'],
  }
}
