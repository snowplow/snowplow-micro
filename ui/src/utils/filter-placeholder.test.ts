import { describe, expect, it } from 'vitest'
import { filterPlaceholder } from './filter-placeholder'

describe('filterPlaceholder', () => {
  it('is the plain label when there is no search limit', () => {
    expect(filterPlaceholder(undefined)).toBe('Filter...')
  })

  it('mentions the search limit when it is set', () => {
    expect(filterPlaceholder(100000)).toBe('Filter (last 100k events)...')
  })

  it('builds the label from the limit rather than a fixed number', () => {
    expect(filterPlaceholder(50000)).toBe('Filter (last 50k events)...')
    expect(filterPlaceholder(2000000)).toBe('Filter (last 2M events)...')
    expect(filterPlaceholder(1500)).toBe('Filter (last 1,500 events)...')
  })
})
