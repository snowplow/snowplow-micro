import { describe, expect, it } from 'vitest'
import {
  DAY_END,
  DAY_START,
  MAX_RELATIVE_MINUTES,
  addDays,
  combineDayAndTime,
  describeTimeFilter,
  formatMinute,
  isSameDay,
  overlapsRange,
  resolveTimeFilter,
  startOfDay,
  toTimeInput,
} from './time-filter'

// Local wall clock, which is what the day strip and the time inputs speak. Expectations
// are built the same way rather than as ISO literals, so they hold in any zone.
const localWallClock = (
  year: number,
  month: number,
  day: number,
  hours = 0,
  minutes = 0,
  seconds = 0,
  ms = 0
) => new Date(year, month - 1, day, hours, minutes, seconds, ms)

describe('combineDayAndTime', () => {
  it('takes the day from the date and the time from the string', () => {
    const day = localWallClock(2026, 3, 14, 17, 45, 30, 123)
    expect(combineDayAndTime(day, '09:05', false)).toBe(
      localWallClock(2026, 3, 14, 9, 5).toISOString()
    )
  })

  it('zeroes seconds and milliseconds so a bound never lands mid-minute', () => {
    const combined = new Date(
      combineDayAndTime(localWallClock(2026, 3, 14, 0, 0, 42, 7), '09:05', false)
    )
    expect(combined.getSeconds()).toBe(0)
    expect(combined.getMilliseconds()).toBe(0)
  })

  // The bound is exclusive, so a plain 23:59 would cut the day's last minute
  it('stretches 23:59 as an end to the end of that minute', () => {
    expect(combineDayAndTime(localWallClock(2026, 3, 14), DAY_END, true)).toBe(
      localWallClock(2026, 3, 14, 23, 59, 59, 999).toISOString()
    )
  })

  it('stays within the day it was given', () => {
    const end = new Date(combineDayAndTime(localWallClock(2026, 3, 14), DAY_END, true))
    expect(isSameDay(end, localWallClock(2026, 3, 14))).toBe(true)
    expect(toTimeInput(end)).toBe(DAY_END)
  })

  it('does not stretch 23:59 when it is a start', () => {
    expect(combineDayAndTime(localWallClock(2026, 3, 14), DAY_END, false)).toBe(
      localWallClock(2026, 3, 14, 23, 59).toISOString()
    )
  })

  it('leaves every other end time on the minute', () => {
    expect(combineDayAndTime(localWallClock(2026, 3, 14), '23:58', true)).toBe(
      localWallClock(2026, 3, 14, 23, 58).toISOString()
    )
    expect(combineDayAndTime(localWallClock(2026, 3, 14), DAY_START, true)).toBe(
      localWallClock(2026, 3, 14).toISOString()
    )
  })

  it('falls back to midnight for an unparseable time', () => {
    expect(combineDayAndTime(localWallClock(2026, 3, 14), '', false)).toBe(
      localWallClock(2026, 3, 14).toISOString()
    )
  })
})

describe('toTimeInput', () => {
  it('pads to the HH:MM an <input type="time"> expects', () => {
    expect(toTimeInput(localWallClock(2026, 3, 14, 9, 5))).toBe('09:05')
    expect(toTimeInput(localWallClock(2026, 3, 14, 0, 0))).toBe('00:00')
    expect(toTimeInput(localWallClock(2026, 3, 14, 23, 59))).toBe('23:59')
  })

  it('is empty without a date, which blanks the field', () => {
    expect(toTimeInput(undefined)).toBe('')
  })

  // The popover reads its fields back off the bounds it emitted, so the pair has to
  // round-trip or picking a second day would move the times
  it('round-trips a whole-day selection', () => {
    const day = localWallClock(2026, 3, 14)
    const start = new Date(combineDayAndTime(day, DAY_START, false))
    const end = new Date(combineDayAndTime(day, DAY_END, true))
    expect([toTimeInput(start), toTimeInput(end)]).toEqual([DAY_START, DAY_END])
  })
})

describe('startOfDay', () => {
  it('clears the time without touching the original', () => {
    const original = localWallClock(2026, 3, 14, 17, 45, 30, 123)
    expect(startOfDay(original)).toEqual(localWallClock(2026, 3, 14))
    expect(original).toEqual(localWallClock(2026, 3, 14, 17, 45, 30, 123))
  })
})

describe('addDays', () => {
  it('crosses month and year boundaries', () => {
    expect(addDays(localWallClock(2026, 1, 31), 1)).toEqual(localWallClock(2026, 2, 1))
    expect(addDays(localWallClock(2026, 12, 31), 1)).toEqual(localWallClock(2027, 1, 1))
    expect(addDays(localWallClock(2026, 3, 1), -1)).toEqual(localWallClock(2026, 2, 28))
  })

  it('lands on Feb 29 in a leap year', () => {
    expect(addDays(localWallClock(2024, 2, 28), 1)).toEqual(localWallClock(2024, 2, 29))
  })

  it('does not mutate its argument', () => {
    const original = localWallClock(2026, 3, 14)
    addDays(original, 5)
    expect(original).toEqual(localWallClock(2026, 3, 14))
  })

  // How the day strip is built: today last, the six preceding days before it
  it('builds a contiguous strip ending today', () => {
    const today = localWallClock(2026, 3, 1)
    const strip = Array.from({ length: 7 }, (_, i) => addDays(today, i - 6))
    expect(strip[0]).toEqual(localWallClock(2026, 2, 23))
    expect(strip[6]).toEqual(today)
  })
})

describe('isSameDay', () => {
  it('compares the day, not the distance', () => {
    expect(isSameDay(localWallClock(2026, 3, 14, 0, 0), localWallClock(2026, 3, 14, 23, 59, 59, 999))).toBe(
      true
    )
    expect(isSameDay(localWallClock(2026, 3, 14, 23, 59), localWallClock(2026, 3, 15, 0, 0))).toBe(false)
  })
})

describe('resolveTimeFilter', () => {
  const anchor = new Date('2026-03-14T12:00:00.000Z')

  it('anchors a relative filter and leaves it open-ended', () => {
    expect(resolveTimeFilter({ kind: 'relative', minutes: 15 }, anchor)).toEqual({
      start: '2026-03-14T11:45:00.000Z',
    })
  })

  it('resolves the widest relative window the charts allow', () => {
    const { start } = resolveTimeFilter(
      { kind: 'relative', minutes: MAX_RELATIVE_MINUTES },
      anchor
    )
    expect(start).toBe('2026-03-07T12:00:00.000Z')
  })

  it('slides with the anchor, which is what keeps paging stable between refreshes', () => {
    const later = new Date(anchor.getTime() + 60_000)
    expect(resolveTimeFilter({ kind: 'relative', minutes: 15 }, later)).toEqual({
      start: '2026-03-14T11:46:00.000Z',
    })
  })

  it('passes an absolute filter through, ignoring the anchor', () => {
    expect(
      resolveTimeFilter({ kind: 'absolute', start: '2026-03-14T00:00:00.000Z' }, anchor)
    ).toEqual({ start: '2026-03-14T00:00:00.000Z', end: undefined })
  })
})

describe('overlapsRange', () => {
  // A day cell in the strip, which is what the range is tested against
  const day: [Date, Date] = [
    new Date('2026-03-14T00:00:00.000Z'),
    new Date('2026-03-15T00:00:00.000Z'),
  ]

  it('treats an unbounded side as reaching forever', () => {
    expect(overlapsRange({}, ...day)).toBe(true)
    expect(overlapsRange({ start: '2020-01-01T00:00:00.000Z' }, ...day)).toBe(true)
    expect(overlapsRange({ end: '2030-01-01T00:00:00.000Z' }, ...day)).toBe(true)
  })

  // Both sides are half-open: a range ending exactly where the day starts holds no
  // instant the day holds, so that cell must not light up
  it('excludes a range that only touches the day at its end', () => {
    expect(overlapsRange({ end: '2026-03-14T00:00:00.000Z' }, ...day)).toBe(false)
  })

  it('excludes a range that only touches the day at its start', () => {
    expect(overlapsRange({ start: '2026-03-15T00:00:00.000Z' }, ...day)).toBe(false)
  })

  it('includes a range overlapping by a single millisecond', () => {
    expect(overlapsRange({ end: '2026-03-14T00:00:00.001Z' }, ...day)).toBe(true)
    expect(overlapsRange({ start: '2026-03-14T23:59:59.999Z' }, ...day)).toBe(true)
  })

  it('includes a range strictly inside the day', () => {
    expect(
      overlapsRange(
        { start: '2026-03-14T10:00:00.000Z', end: '2026-03-14T11:00:00.000Z' },
        ...day
      )
    ).toBe(true)
  })

  // The time fields let one be typed, and it matches nothing
  it('excludes an inverted range', () => {
    expect(
      overlapsRange(
        { start: '2026-03-14T11:00:00.000Z', end: '2026-03-14T10:00:00.000Z' },
        ...day
      )
    ).toBe(false)
  })
})

describe('formatMinute', () => {
  it('uses a 24-hour clock', () => {
    expect(formatMinute(localWallClock(2026, 3, 14, 13, 5))).toBe('13:05')
  })

  // `hour12: false` resolves to h24 on some ICU builds, which writes midnight as 24:00
  // without rolling the date back, so the same label means two different days
  it('writes midnight as 00:00, not 24:00', () => {
    expect(formatMinute(localWallClock(2026, 3, 14, 0, 0))).toBe('00:00')
    expect(formatMinute(localWallClock(2026, 3, 14, 0, 30))).toBe('00:30')
  })

  it('writes the last minute of the day as 23:59', () => {
    expect(formatMinute(localWallClock(2026, 3, 14, 23, 59))).toBe('23:59')
  })
})

describe('describeTimeFilter', () => {
  it('reads as unfiltered when there is nothing to describe', () => {
    expect(describeTimeFilter(null)).toBe('Any time')
    expect(describeTimeFilter({ kind: 'absolute' })).toBe('Any time')
  })

  it('names the preset for a relative filter', () => {
    expect(describeTimeFilter({ kind: 'relative', minutes: 15 })).toBe('Last 15 min')
  })

  it('drops the repeated date when both bounds fall on one day', () => {
    expect(
      describeTimeFilter({
        kind: 'absolute',
        start: localWallClock(2026, 3, 14, 9, 0).toISOString(),
        end: localWallClock(2026, 3, 14, 17, 30).toISOString(),
      })
    ).toBe('Mar 14, 09:00 – 17:30')
  })

  it('spells out both dates when the range spans days', () => {
    expect(
      describeTimeFilter({
        kind: 'absolute',
        start: localWallClock(2026, 3, 14, 9, 0).toISOString(),
        end: localWallClock(2026, 3, 15, 17, 30).toISOString(),
      })
    ).toBe('Mar 14, 09:00 – Mar 15, 17:30')
  })

  it('describes a whole day by its own date, not the next one', () => {
    const day = localWallClock(2026, 3, 14)
    expect(
      describeTimeFilter({
        kind: 'absolute',
        start: combineDayAndTime(day, DAY_START, false),
        end: combineDayAndTime(day, DAY_END, true),
      })
    ).toBe('Mar 14, 00:00 – 23:59')
  })

  it('describes a one-sided range by the bound it has', () => {
    const bound = localWallClock(2026, 3, 14, 9, 0).toISOString()
    expect(describeTimeFilter({ kind: 'absolute', start: bound })).toBe(
      'After Mar 14, 09:00'
    )
    expect(describeTimeFilter({ kind: 'absolute', end: bound })).toBe(
      'Before Mar 14, 09:00'
    )
  })
})
