import type { ColumnFiltersState } from '@tanstack/react-table'
import type { ColumnMetadata } from '@/utils/column-metadata'
import { isFixedColumn } from '@/utils/fixed-columns'
import { MAX_RELATIVE_MINUTES, type TimeFilter } from '@/utils/time-filter'

export type UrlViewState = {
  columns: string[]
  filters: ColumnFiltersState
  timeFilter: TimeFilter | null
}

const RELATIVE_TIME = /^last(\d+)m$/

const timestamp = (value: string): string | undefined =>
  value && !Number.isNaN(new Date(value).getTime()) ? value : undefined

export function parseViewUrl(): UrlViewState | null {
  const params = new URLSearchParams(window.location.search)

  const columns: string[] = []
  const filters: ColumnFiltersState = []
  let timeFilter: TimeFilter | null = null

  for (const [key, value] of params) {
    if (key === 'status') {
      if (value) {
        filters.push({ id: 'status', value })
      }
    } else if (key === 'time') {
      const relative = RELATIVE_TIME.exec(value)
      if (relative) {
        const minutes = Number(relative[1])
        if (minutes > 0 && minutes <= MAX_RELATIVE_MINUTES) {
          timeFilter = { kind: 'relative', minutes }
        }
      } else {
        const parts = value.split('~')
        const start = timestamp(parts[0] ?? '')
        const end = timestamp(parts[1] ?? '')
        if (start || end) {
          timeFilter = { kind: 'absolute', start, end }
        }
      }
    } else if (!isFixedColumn(key)) {
      columns.push(key)
      if (value) {
        filters.push({ id: key, value: value.split('~') })
      }
    }
  }

  if (columns.length === 0 && filters.length === 0 && !timeFilter) return null
  return { columns, filters, timeFilter }
}

export function serializeViewUrl(
  selectedColumns: ColumnMetadata[],
  columnFilters: ColumnFiltersState,
  timeFilter: TimeFilter | null,
): string {
  const parts: string[] = []

  for (const col of selectedColumns) {
    const filter = columnFilters.find((f) => f.id === col.name)
    const encodedName = encodeURIComponent(col.name)
    if (filter && filter.value) {
      const values = Array.isArray(filter.value)
        ? (filter.value as string[])
        : [String(filter.value)]
      if (values.length > 0) {
        parts.push(`${encodedName}=${values.map((v) => encodeURIComponent(v)).join('~')}`)
      } else {
        parts.push(`${encodedName}=`)
      }
    } else {
      parts.push(`${encodedName}=`)
    }
  }

  const statusFilter = columnFilters.find((f) => f.id === 'status')
  if (statusFilter?.value) {
    parts.push(`status=${encodeURIComponent(String(statusFilter.value))}`)
  }

  if (timeFilter?.kind === 'relative') {
    parts.push(`time=last${timeFilter.minutes}m`)
  } else if (timeFilter) {
    parts.push(
      `time=${encodeURIComponent(timeFilter.start ?? '')}~${encodeURIComponent(timeFilter.end ?? '')}`
    )
  }

  return `${window.location.origin}${window.location.pathname}?${parts.join('&')}`
}
