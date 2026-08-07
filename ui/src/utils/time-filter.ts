import type { TimeRange } from '@/services/api'

// A relative filter re-anchors to the latest refresh, so "last 15 min" keeps sliding;
// an absolute one is a fixed range (a clicked chart bucket or hand-picked days).
export type TimeFilter =
  | { kind: 'absolute'; start?: string; end?: string }
  | { kind: 'relative'; minutes: number }

// Capped by the minutes chart, which shows 30 one-minute buckets
export const TIME_PRESETS = [5, 15, 30]

// The days chart spans a week (28 six-hour buckets, see generateCombinedTimeline),
// so the day strip offers the same window
export const STRIP_DAYS = 7

// Bounded by what the charts and the day strip can show
export const MAX_RELATIVE_MINUTES = STRIP_DAYS * 24 * 60

export const isSameDay = (a: Date, b: Date) => a.toDateString() === b.toDateString()

export function resolveTimeFilter(filter: TimeFilter, anchor: Date): TimeRange {
  if (filter.kind === 'relative') {
    return { start: new Date(anchor.getTime() - filter.minutes * 60_000).toISOString() }
  }
  return { start: filter.start, end: filter.end }
}

// Both the range and the span are half-open, matching the backend's
// `timestamp >= start AND timestamp < end`
export function overlapsRange(range: TimeRange, spanStart: Date, spanEnd: Date): boolean {
  const start = range.start ? new Date(range.start).getTime() : -Infinity
  const end = range.end ? new Date(range.end).getTime() : Infinity
  // The time fields allow an inverted range. It holds no instant at all, so nothing
  // overlaps it — without this the day strip would light up for a filter matching nothing
  if (start >= end) return false
  return spanEnd.getTime() > start && spanStart.getTime() < end
}

export function presetLabel(minutes: number): string {
  return `Last ${minutes} min`
}

// The time fields are `<input type="time">`, so bounds are picked as local wall clock
export const DAY_START = '00:00'
export const DAY_END = '23:59'

const pad = (n: number) => String(n).padStart(2, '0')

export function toTimeInput(date?: Date): string {
  return date ? `${pad(date.getHours())}:${pad(date.getMinutes())}` : ''
}

export function startOfDay(date: Date): Date {
  const day = new Date(date)
  day.setHours(0, 0, 0, 0)
  return day
}

export function addDays(date: Date, count: number): Date {
  const shifted = new Date(date)
  shifted.setDate(date.getDate() + count)
  return shifted
}

// The day strip picks the day, the time fields pick the time within it. The end bound is
// exclusive, so as an end 23:59 has to stretch to the end of that minute or a whole-day
// selection would drop its final minute. It stops a millisecond short of the next day so
// that reading the bound back still lands on the day the user picked.
export function combineDayAndTime(day: Date, time: string, isEnd: boolean): string {
  const [hours, minutes] = time.split(':').map(Number)
  const endOfDay = isEnd && time === DAY_END
  const combined = new Date(day)
  combined.setHours(hours || 0, minutes || 0, endOfDay ? 59 : 0, endOfDay ? 999 : 0)
  return combined.toISOString()
}

// Every time label in the UI reads the same way, charts included
export const LOCALE = 'en-US'
const DATE_OPTS: Intl.DateTimeFormatOptions = { month: 'short', day: 'numeric' }
const TIME_OPTS: Intl.DateTimeFormatOptions = {
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
}

export function formatDay(date: Date): string {
  return date.toLocaleDateString(LOCALE, DATE_OPTS)
}

// Chart axis labels: the days chart is bucketed by hour, the minutes chart by minute
export function formatDayHour(date: Date): string {
  return date.toLocaleDateString(LOCALE, { ...DATE_OPTS, hour: '2-digit' })
}

export function formatMinute(date: Date): string {
  return date.toLocaleTimeString(LOCALE, TIME_OPTS)
}

function formatStamp(date: Date): string {
  return `${formatDay(date)}, ${formatMinute(date)}`
}

export function describeTimeFilter(filter: TimeFilter | null): string {
  if (!filter) return 'Any time'
  if (filter.kind === 'relative') return presetLabel(filter.minutes)

  const start = filter.start ? new Date(filter.start) : null
  const end = filter.end ? new Date(filter.end) : null

  if (start && end) {
    return isSameDay(start, end)
      ? `${formatStamp(start)} – ${end.toLocaleTimeString(LOCALE, TIME_OPTS)}`
      : `${formatStamp(start)} – ${formatStamp(end)}`
  }
  if (start) return `After ${formatStamp(start)}`
  if (end) return `Before ${formatStamp(end)}`
  return 'Any time'
}
