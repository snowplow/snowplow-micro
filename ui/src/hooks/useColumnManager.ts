import { useState, useMemo, useEffect } from 'react'

import { type Event } from '@/services/api'
import type { ColumnFiltersState, OnChangeFn } from '@tanstack/react-table'
import { discoverAllFields } from '@/utils/json-fields'
import { type ColumnMetadata, createColumnMetadata } from '@/utils/column-metadata'

const STORAGE_KEY = 'snowplow-micro-selected-columns'

// Default column configuration
const DEFAULT_COLUMNS = ['collector_tstamp', 'app_id', 'event_name']

/**
 * Save selected columns to localStorage
 */
function saveSelectedColumns(columns: string[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(columns))
  } catch (error) {
    console.warn('Failed to save selected columns to localStorage:', error)
  }
}

/**
 * Load selected columns from localStorage
 */
function loadSelectedColumns(): string[] {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      const parsed = JSON.parse(stored)
      if (
        Array.isArray(parsed) &&
        parsed.every((item) => typeof item === 'string')
      ) {
        return parsed
      }
    }
  } catch (error) {
    console.warn('Failed to load selected columns from localStorage:', error)
  }

  return [...DEFAULT_COLUMNS]
}

type UseColumnManagerProps = {
  events: Event[]
  setColumnFilters: OnChangeFn<ColumnFiltersState>
  onColumnAdded?: () => void
}

type UseColumnManagerReturn = {
  availableColumns: ColumnMetadata[]
  selectedColumns: ColumnMetadata[]
  toggleColumn: (fieldName: string) => void
  reorderColumns: (fromIndex: number, toIndex: number) => void
  resetToDefaults: () => void
}

export function useColumnManager({
  events,
  setColumnFilters,
  onColumnAdded,
}: UseColumnManagerProps): UseColumnManagerReturn {
  const [selectedColumnNames, setSelectedColumnNames] = useState<string[]>(
    loadSelectedColumns()
  )

  useEffect(() => {
    saveSelectedColumns(selectedColumnNames)
  }, [selectedColumnNames])

  const selectedColumns = useMemo(() => {
    return selectedColumnNames.map(createColumnMetadata)
  }, [selectedColumnNames])

  const availableColumns = useMemo(() => {
    const parentsOfSelected = selectedColumns
      .filter(col => col.isNested)
      .map(col => col.parentColumn!)

    const set = new Set([
      ...selectedColumnNames,
      ...parentsOfSelected,
      ...discoverAllFields(events),
    ])

    // Remove failure entity, br_ fields, and all their nested fields
    const filteredColumnNames = Array.from(set).filter(
      (columnName) =>
        !columnName.startsWith(
          'contexts_com_snowplowanalytics_snowplow_failure_1'
        ) && !columnName.startsWith('br_')
    )

    return filteredColumnNames.map(createColumnMetadata)
  }, [events, selectedColumnNames])

  const toggleColumn = (fieldName: string) => {
    if (selectedColumnNames.includes(fieldName)) {
      const updated = selectedColumnNames.filter((c) => c != fieldName)
      setColumnFilters((filters) => filters.filter((f) => f.id !== fieldName))
      setSelectedColumnNames(updated)
    } else {
      const updated = [...selectedColumnNames, fieldName]
      setSelectedColumnNames(updated)
      // Notify that a column was added
      onColumnAdded?.()
    }
  }

  const reorderColumns = (fromIndex: number, toIndex: number) => {
    const reordered = [...selectedColumnNames]
    const column = reordered.splice(fromIndex, 1)[0]
    reordered.splice(toIndex, 0, column)
    setSelectedColumnNames(reordered)
  }

  const resetToDefaults = () => {
    const removedColumns = selectedColumnNames.filter(col => !DEFAULT_COLUMNS.includes(col))
    setColumnFilters((filters) => filters.filter((f) => !removedColumns.includes(f.id)))
    setSelectedColumnNames([...DEFAULT_COLUMNS])
  }

  return {
    availableColumns,
    selectedColumns,
    toggleColumn,
    reorderColumns,
    resetToDefaults,
  }
}
