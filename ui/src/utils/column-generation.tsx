import type { ColumnDef } from '@tanstack/react-table'
import { Button } from '@/components/ui/button'
import { ArrowUp, ArrowDown, Eye, Check, X } from 'lucide-react'
import type { Event, ColumnStats } from '@/services/api'
import { DraggableColumn } from '@/components/DraggableColumn'
import { TruncatedCell } from '@/components/TruncatedCell'
import { TruncatedColumnName } from '@/components/TruncatedColumnName'
import { truncateJsonForDisplay } from './json-fields'
import { type ColumnMetadata } from './column-metadata'
import { hasFailureData } from './event-utils'

export type EventColumnMeta = {
  eventStatusFilter?: boolean
  useAutocomplete?: boolean
  distinctValues?: string[]
}

export type EventColumnDef = ColumnDef<Event> & {
  meta?: EventColumnMeta
}

/**
 * Generate columns from selected fields and available field info
 */
export function generateColumns(
  selectedColumns: ColumnMetadata[],
  selectedCellId: string | null,
  onJsonCellToggle: (cellId: string, value: any, title: string) => void,
  onReorderColumns: (fromIndex: number, toIndex: number) => void,
  columnStats: Record<string, ColumnStats>
): EventColumnDef[] {
  const columns: EventColumnDef[] = []

  // Pinned status column
  columns.push({
    id: 'status',
    accessorFn: (row) => (hasFailureData(row) ? 'failed' : 'valid'),
    header: () => <div className="text-center">Status</div>,
    meta: {
      eventStatusFilter: true,
    },
    cell: ({ row }) => {
      const hasFailures = hasFailureData(row.original)
      const cellId = `${row.original.event_id}-status`
      const isSelected = selectedCellId === cellId

      if (!hasFailures) {
        return (
          <div className="px-2 py-1 flex justify-center">
            <span className="px-3 py-1 rounded-full text-xs whitespace-nowrap bg-success-light text-success-dark font-normal flex items-center gap-1">
              <Check className="h-3 w-3" />
              Valid
            </span>
          </div>
        )
      }

      const failureData =
        row.original.contexts_com_snowplowanalytics_snowplow_failure_1
      if (Array.isArray(failureData) && onJsonCellToggle) {
        const failureCount = failureData.length
        return (
          <div
            className="px-2 py-1 flex items-center justify-center gap-2 cursor-pointer group"
            onClick={() =>
              onJsonCellToggle(
                cellId,
                failureData.map((v) => ({
                  data: v.data,
                  errors: v.errors,
                  failureType: v.failureType,
                })),
                'Failures'
              )
            }
            data-cell-clickable="true"
          >
            <span
              className={`px-2 py-1 rounded-full text-xs font-normal whitespace-nowrap transition-colors flex items-center gap-1 ${
                isSelected
                  ? 'bg-failure text-failure-dark'
                  : 'bg-failure-light text-failure-dark group-hover:bg-failure'
              }`}
            >
              <X className="h-3 w-3" />
              {failureCount} {failureCount === 1 ? 'failure' : 'failures'}
            </span>
          </div>
        )
      }

      return <div className="px-2 py-1">Failed</div>
    },
    enableSorting: false,
    enableColumnFilter: true,
  })

  // Add selected columns
  selectedColumns.forEach((columnMetadata, index) => {
    const { name: fieldName } = columnMetadata

    // Use backend-provided sortable/filterable flags
    const stats = columnStats[fieldName]
    const isSortable = stats?.sortable ?? false
    const isFilterable = stats?.filterable ?? false
    const distinctValues = stats?.values ?? []
    const useAutocomplete = distinctValues.length > 0

    const columnDef: EventColumnDef = {
      id: fieldName,
      accessorFn: columnMetadata.accessor,
      meta: {
        useAutocomplete,
        distinctValues,
      },
      header: ({ column }) => {
        return (
          <DraggableColumn index={index} onReorder={onReorderColumns}>
            {!isSortable ? (
              <TruncatedColumnName
                className="px-2"
                columnMetadata={columnMetadata}
              />
            ) : (
              <Button
                variant="ghost"
                onClick={() =>
                  column.toggleSorting(column.getIsSorted() !== 'desc')
                }
                className="h-8 px-2 flex items-center group"
                title="Sort column"
              >
                <TruncatedColumnName columnMetadata={columnMetadata} />
                <div className="ml-2">
                  {column.getIsSorted() === 'asc' ? (
                    <ArrowUp className="h-4 w-4" />
                  ) : column.getIsSorted() === 'desc' ? (
                    <ArrowDown className="h-4 w-4" />
                  ) : (
                    <ArrowDown className="h-4 w-4 opacity-0 group-hover:opacity-100 transition-opacity" />
                  )}
                </div>
              </Button>
            )}
          </DraggableColumn>
        )
      },
      cell: ({ getValue, row }) => {
        const value = getValue()

        // Handle undefined values - show empty cell
        if (value === undefined) {
          return <div className="px-2 py-1"></div>
        }

        const cellId = `${row.original.event_id}-${fieldName}`
        const isSelected = selectedCellId === cellId

        if (columnMetadata.isJSON) {
          // For nested fields, show the parent column data in the JSON viewer
          const jsonViewerData = columnMetadata.isNested
            ? row.original[columnMetadata.parentColumn!]
            : value
          const jsonViewerTitle = columnMetadata.isNested
            ? columnMetadata.parentColumn!
            : fieldName

          return (
            <div className="px-2 py-1 flex justify-start">
              <div
                className={`flex items-center gap-2 cursor-pointer hover:bg-gray-200 rounded px-2 py-0.5 ${isSelected ? 'bg-gray-200' : ''}`}
                onClick={() =>
                  onJsonCellToggle(cellId, jsonViewerData, jsonViewerTitle)
                }
                data-cell-clickable="true"
              >
                <Eye className="h-4 w-4 text-muted-foreground" />
                <span className="text-sm whitespace-pre-wrap">
                  {truncateJsonForDisplay(value, fieldName, 100)}
                </span>
              </div>
            </div>
          )
        }

        if (columnMetadata.isTimestamp) {
          let formatted = String(value)
          try {
            const date = new Date(value as any)
            formatted = date.toLocaleString()
          } catch {
            // nothing to do
          }
          return <div className="px-2 py-1">{formatted}</div>
        }

        // Regular formatting for primitive values - use TruncatedCell for strings
        return <TruncatedCell value={String(value)} />
      },
      enableSorting: isSortable,
      enableColumnFilter: isFilterable,
    }

    columns.push(columnDef)
  })

  return columns
}
