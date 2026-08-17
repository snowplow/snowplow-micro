import { afterEach, describe, expect, it, vi } from 'vitest'
import type { ColumnFiltersState } from '@tanstack/react-table'
import { createColumnMetadata } from './column-metadata'
import { FAILURE_COLUMN, TIMESTAMP_COLUMN } from './fixed-columns'
import { MAX_RELATIVE_MINUTES, type TimeFilter } from './time-filter'
import { parseViewUrl, serializeViewUrl } from './view-url'

const at = (url: string) => {
  const { search, origin, pathname } = new URL(url)
  vi.stubGlobal('window', { location: { search, origin, pathname } })
}

const BASE = 'https://micro.example/micro/ui'

const parse = (query: string) => {
  at(`${BASE}${query}`)
  return parseViewUrl()
}

const serialize = (
  columns: string[],
  filters: ColumnFiltersState = [],
  timeFilter: TimeFilter | null = null
) => {
  at(BASE)
  return serializeViewUrl(columns.map(createColumnMetadata), filters, timeFilter)
}

const query = (url: string) => url.slice(`${BASE}?`.length)

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('parseViewUrl', () => {
  it('is null when there is nothing to restore', () => {
    expect(parse('')).toBeNull()
    expect(parse('?')).toBeNull()
  })

  it('reads a column with no filter as selected but unfiltered', () => {
    expect(parse('?app_id=')).toEqual({
      columns: ['app_id'],
      filters: [],
      timeFilter: null,
    })
  })

  it('splits a multi-value column filter on ~', () => {
    expect(parse('?app_id=web~mobile')).toEqual({
      columns: ['app_id'],
      filters: [{ id: 'app_id', value: ['web', 'mobile'] }],
      timeFilter: null,
    })
  })

  it('keeps column order, which is the order they are shown in', () => {
    expect(parse('?b=&a=&c=')?.columns).toEqual(['b', 'a', 'c'])
  })

  it('carries the status filter as a scalar, not a column', () => {
    expect(parse('?status=good')).toEqual({
      columns: [],
      filters: [{ id: 'status', value: 'good' }],
      timeFilter: null,
    })
  })

  it('ignores an empty status', () => {
    expect(parse('?status=')).toBeNull()
  })

  it('restores a filter-only view with no selected columns', () => {
    expect(parse('?status=bad')).not.toBeNull()
    expect(parse('?time=last15m')).not.toBeNull()
  })

  it('drops references to fixed columns and their nested fields', () => {
    expect(parse(`?${TIMESTAMP_COLUMN}=&app_id=`)?.columns).toEqual(['app_id'])
    expect(parse(`?${FAILURE_COLUMN}.errors=&app_id=`)?.columns).toEqual(['app_id'])
  })

  it('drops a fixed column even when it carries a filter', () => {
    expect(parse(`?${FAILURE_COLUMN}=oops`)).toBeNull()
  })

  it('decodes percent-encoded column names and values', () => {
    expect(parse('?unstruct_event_x.a%20b=one%20two')).toEqual({
      columns: ['unstruct_event_x.a b'],
      filters: [{ id: 'unstruct_event_x.a b', value: ['one two'] }],
      timeFilter: null,
    })
  })

  describe('the time parameter', () => {
    it('reads a relative window', () => {
      expect(parse('?time=last15m')?.timeFilter).toEqual({
        kind: 'relative',
        minutes: 15,
      })
    })

    it('accepts the widest window the charts can show', () => {
      expect(parse(`?time=last${MAX_RELATIVE_MINUTES}m`)?.timeFilter).toEqual({
        kind: 'relative',
        minutes: MAX_RELATIVE_MINUTES,
      })
    })

    it('rejects a window of zero or beyond the maximum', () => {
      expect(parse('?time=last0m')).toBeNull()
      expect(parse(`?time=last${MAX_RELATIVE_MINUTES + 1}m`)).toBeNull()
    })

    it('does not mistake a near-miss of the relative form for a range', () => {
      expect(parse('?time=last15')).toBeNull()
      expect(parse('?time=last-5m')).toBeNull()
      expect(parse('?time=lastm')).toBeNull()
    })

    it('reads an absolute range', () => {
      expect(
        parse('?time=2026-03-14T00%3A00%3A00.000Z~2026-03-15T00%3A00%3A00.000Z')
          ?.timeFilter
      ).toEqual({
        kind: 'absolute',
        start: '2026-03-14T00:00:00.000Z',
        end: '2026-03-15T00:00:00.000Z',
      })
    })

    it('reads a one-sided range from either side', () => {
      expect(parse('?time=2026-03-14T00%3A00%3A00.000Z~')?.timeFilter).toEqual({
        kind: 'absolute',
        start: '2026-03-14T00:00:00.000Z',
        end: undefined,
      })
      expect(parse('?time=~2026-03-15T00%3A00%3A00.000Z')?.timeFilter).toEqual({
        kind: 'absolute',
        start: undefined,
        end: '2026-03-15T00:00:00.000Z',
      })
    })

    it('drops a bound that is not a date, keeping the one that is', () => {
      expect(parse('?time=nonsense~2026-03-15T00%3A00%3A00.000Z')?.timeFilter).toEqual({
        kind: 'absolute',
        start: undefined,
        end: '2026-03-15T00:00:00.000Z',
      })
    })

    it('ignores the filter when neither bound is a date', () => {
      expect(parse('?time=nonsense~rubbish')).toBeNull()
      expect(parse('?time=')).toBeNull()
      expect(parse('?time=~')).toBeNull()
    })
  })
})

describe('serializeViewUrl', () => {
  it('keeps the current page as the base', () => {
    expect(serialize(['app_id'])).toBe(`${BASE}?app_id=`)
  })

  it('writes a selected column with no filter as a bare name', () => {
    expect(query(serialize(['app_id', 'platform']))).toBe('app_id=&platform=')
  })

  it('joins multi-value filters with ~', () => {
    expect(
      query(serialize(['app_id'], [{ id: 'app_id', value: ['web', 'mobile'] }]))
    ).toBe('app_id=web~mobile')
  })

  it('ignores filters on columns that are not selected', () => {
    expect(query(serialize(['app_id'], [{ id: 'platform', value: ['web'] }]))).toBe(
      'app_id='
    )
  })

  it('puts status after the columns, whether or not any are selected', () => {
    expect(query(serialize(['app_id'], [{ id: 'status', value: 'bad' }]))).toBe(
      'app_id=&status=bad'
    )
    expect(query(serialize([], [{ id: 'status', value: 'bad' }]))).toBe('status=bad')
  })

  it('writes a relative filter as the sliding form, not as resolved bounds', () => {
    expect(query(serialize([], [], { kind: 'relative', minutes: 15 }))).toBe(
      'time=last15m'
    )
  })

  it('writes both bounds of an absolute filter, keeping the separator when one is missing', () => {
    expect(
      query(
        serialize([], [], {
          kind: 'absolute',
          start: '2026-03-14T00:00:00.000Z',
          end: '2026-03-15T00:00:00.000Z',
        })
      )
    ).toBe('time=2026-03-14T00%3A00%3A00.000Z~2026-03-15T00%3A00%3A00.000Z')

    expect(
      query(serialize([], [], { kind: 'absolute', start: '2026-03-14T00:00:00.000Z' }))
    ).toBe('time=2026-03-14T00%3A00%3A00.000Z~')
  })

  it('leaves the time parameter out when nothing is filtered', () => {
    expect(query(serialize(['app_id'], [], null))).toBe('app_id=')
  })

  it('escapes the separator characters in names and values', () => {
    const url = query(serialize(['a&b=c'], [{ id: 'a&b=c', value: ['x y'] }]))
    expect(url).toBe('a%26b%3Dc=x%20y')
  })
})

describe('a view round-trips through the URL', () => {
  const roundTrip = (
    columns: string[],
    filters: ColumnFiltersState,
    timeFilter: TimeFilter | null
  ) => {
    const url = serializeViewUrl(columns.map(createColumnMetadata), filters, timeFilter)
    return parse(`?${query(url)}`)
  }

  it('preserves columns, filters and a relative time filter', () => {
    at(BASE)
    expect(
      roundTrip(
        ['app_id', 'platform'],
        [
          { id: 'app_id', value: ['web', 'mobile'] },
          { id: 'status', value: 'bad' },
        ],
        { kind: 'relative', minutes: 30 }
      )
    ).toEqual({
      columns: ['app_id', 'platform'],
      filters: [
        { id: 'app_id', value: ['web', 'mobile'] },
        { id: 'status', value: 'bad' },
      ],
      timeFilter: { kind: 'relative', minutes: 30 },
    })
  })

  it('preserves an absolute time filter and awkward names', () => {
    at(BASE)
    expect(
      roundTrip(
        ['unstruct_event_my_schema_1.a b'],
        [{ id: 'unstruct_event_my_schema_1.a b', value: ['one two', 'three&four'] }],
        { kind: 'absolute', start: '2026-03-14T00:00:00.000Z' }
      )
    ).toEqual({
      columns: ['unstruct_event_my_schema_1.a b'],
      filters: [
        { id: 'unstruct_event_my_schema_1.a b', value: ['one two', 'three&four'] },
      ],
      timeFilter: { kind: 'absolute', start: '2026-03-14T00:00:00.000Z', end: undefined },
    })
  })
})
