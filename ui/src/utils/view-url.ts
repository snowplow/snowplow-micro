import type { ColumnFiltersState } from '@tanstack/react-table'
import type { ColumnMetadata } from '@/utils/column-metadata'

export type UrlViewState = {
  columns: string[]
  filters: ColumnFiltersState
  timeBucket: string | null
}

export function parseViewUrl(): UrlViewState | null {
  const params = new URLSearchParams(window.location.search)

  const columns: string[] = []
  const filters: ColumnFiltersState = []
  let timeBucket: string | null = null

  for (const [key, value] of params) {
    if (key === 'status') {
      if (value) {
        filters.push({ id: 'status', value })
      }
    } else if (key === 'time') {
      const parts = value.split('~')
      if (parts.length === 2) {
        timeBucket = `${parts[0]}|${parts[1]}`
      }
    } else {
      columns.push(key)
      if (value) {
        filters.push({ id: key, value: value.split('~') })
      }
    }
  }

  if (columns.length === 0) return null
  return { columns, filters, timeBucket }
}

export function serializeViewUrl(
  selectedColumns: ColumnMetadata[],
  columnFilters: ColumnFiltersState,
  selectedTimeBucket: string | null,
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

  if (selectedTimeBucket) {
    const [start, end] = selectedTimeBucket.split('|')
    parts.push(`time=${encodeURIComponent(start)}~${encodeURIComponent(end)}`)
  }

  return `${window.location.origin}${window.location.pathname}?${parts.join('&')}`
}
