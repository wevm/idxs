import { Foo } from 'supin'

describe('foo', () => {
  test('default', () => {
    expect(Foo.foo()).toBe('Hello, foo!')
  })
})
