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
  return spanEnd.getTime() > start && spanStart.getTime() < end
}

export function presetLabel(minutes: number): string {
  return `Last ${minutes} min`
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
