import { EventEmitter } from 'eventemitter3'

export type Emitter = {
  instance: () => {
    emit: EventEmitter<EventTypes>['emit']
  }
  on: EventEmitter<EventTypes<true>>['on']
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

export type EventTypes<includeOptions extends boolean = false> = {
  debug: (
    event: Events[number]['event'],
    data: Events[number]['data'],
    ...rest: includeOptions extends true ? readonly [{ id: string }] : readonly []
  ) => void
} & {
  [K in Events[number] as K['event']]: (
    parameters: K['data'],
    ...rest: includeOptions extends true ? readonly [{ id: string }] : readonly []
  ) => void
}

export function create(): Emitter {
  const e = new EventEmitter<EventTypes<true>>()
  let id = -1

  function instance() {
    id++
    return {
      emit: ((...args) => {
        // biome-ignore lint/suspicious/noExplicitAny: _
        ;(e as any).emit('debug', args[0], args[1], { id })
        // biome-ignore lint/suspicious/noExplicitAny: _
        return (e as any).emit(args[0], args[1], { id })
      }) satisfies EventEmitter<EventTypes>['emit'],
    }
  }

  return {
    instance,
    on: e.on.bind(e) as never,
  }
}
