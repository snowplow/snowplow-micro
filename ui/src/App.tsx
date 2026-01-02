import { useState, useEffect } from 'react'
import { useAuth } from '@/contexts/AuthContext'
import { DataTable } from '@/components/DataTable'
import { ColumnSelector } from '@/components/ColumnSelector'
import { JsonSidePanel } from '@/components/JsonSidePanel'
import { EventsChart } from '@/components/EventsChart'
import {
  type Event,
  type TimelineData,
  type ColumnStats,
  type EventsRequest,
  type EventsResponse,
  type EventsFilter,
  EventsApiService,
} from '@/services/api'
import { useColumnManager } from '@/hooks/useColumnManager'
import { Button } from '@/components/ui/button'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  RefreshCw,
  Trash2,
  Columns3Cog,
  FilterX,
  MoreVertical,
} from 'lucide-react'
import { type ColumnFiltersState, type SortingState } from '@tanstack/react-table'

function App() {
  const { isAuthenticated, isLoading: authIsLoading, error, getAccessToken } = useAuth()
  const [eventData, setEventData] = useState<EventsResponse>({
    events: [],
    totalPages: 0,
    totalItems: 0,
  })
  const [currentPage, setCurrentPage] = useState(1)
  const [minutesTimelineData, setMinutesTimelineData] = useState<TimelineData>({ points: [] })
  const [daysTimelineData, setDaysTimelineData] = useState<TimelineData>({ points: [] })
  const [availableColumnNames, setAvailableColumnNames] = useState<string[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isRefreshing, setIsRefreshing] = useState(false)
  const [showColumnSelector, setShowColumnSelector] = useState(false)
  const [selectedCellId, setSelectedCellId] = useState<string | null>(null)
  const [selectedRowId, setSelectedRowId] = useState<string | null>(null)
  const [jsonPanelData, setJsonPanelData] = useState<{
    value: any
    title: string
  } | null>(null)
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
  const [selectedTimeBucket, setSelectedTimeBucket] = useState<string | null>(null)
  const [lastRefreshTime, setLastRefreshTime] = useState<Date | null>(null)
  const [showActionsMenu, setShowActionsMenu] = useState(false)
  const [columnStats, setColumnStats] = useState<Record<string, ColumnStats>>(
    {}
  )
  const [sorting, setSorting] = useState<SortingState>([])

  // Handle scrolling to newly added columns
  const scrollToLastColumn = () => {
    setTimeout(() => {
      const headers = document.querySelectorAll('thead th')
      const lastHeader = headers[headers.length - 1]
      if (lastHeader) {
        lastHeader.scrollIntoView({
          behavior: 'smooth',
          block: 'nearest',
          inline: 'nearest',
        })
      }
    }, 100)
  }

  // Update column stats when columns change
  const updateColumnStats = async (columnNames: string[]) => {
    try {
      const token = await getAccessToken()
      const response = await EventsApiService.fetchColumnStats(columnNames, token)
      setColumnStats(response)
    } catch (err) {
      console.warn('Failed to fetch column stats:', err)
    }
  }

  const {
    availableColumns,
    selectedColumns,
    toggleColumn,
    reorderColumns,
    resetToDefaults,
  } = useColumnManager({
    availableColumnNames,
    setColumnFilters,
    onColumnAdded: (columnNames) => {
      scrollToLastColumn()
      updateColumnStats(columnNames)
    },
  })

  // Set initial sorting when collector_tstamp is available
  useEffect(() => {
    if (sorting.length === 0 && selectedColumns.some(col => col.name === 'collector_tstamp')) {
      setSorting([{ id: 'collector_tstamp', desc: true }])
    }
  }, [selectedColumns, sorting])

  const buildEventsRequest = (isRefresh = false): EventsRequest => {
    // Separate status filter from column filters
    const statusFilter = columnFilters.find(f => f.id === 'status')
    const regularFilters = columnFilters.filter(f => f.id !== 'status')

    const filters: EventsFilter[] = regularFilters.map((filter) => ({
      column: filter.id,
      value: String(filter.value),
    }))

    // Map status filter to validEvents field
    const validEvents = statusFilter
      ? statusFilter.value === 'valid'
        ? true
        : statusFilter.value === 'failed'
        ? false
        : undefined
      : undefined

    let timeRange
    if (isRefresh) {
      // only filter by selected bucket, or not at all
      timeRange = selectedTimeBucket
        ? {
            start: selectedTimeBucket.split('|')[0],
            end: selectedTimeBucket.split('|')[1],
          }
        : undefined
    } else {
      // add a (non-inclusive) upper bound to avoid getting newer events in the results
      if (selectedTimeBucket && lastRefreshTime) {
        const bucketStart = selectedTimeBucket.split('|')[0]
        const bucketEnd = selectedTimeBucket.split('|')[1]
        const bucketEndTime = new Date(bucketEnd)
        timeRange = {
          start: bucketStart,
          end: bucketEndTime < lastRefreshTime ? bucketEnd : lastRefreshTime.toISOString(),
        }
      } else if (lastRefreshTime) {
        timeRange = { end: lastRefreshTime.toISOString() }
      }
    }

    return {
      filters,
      validEvents,
      timeRange,
      sorting: sorting.length > 0 ? { column: sorting[0].id, desc: sorting[0].desc } : undefined,
      page: currentPage,
      pageSize: 50,
    }
  }

  const fetchEventsWithFilters = async () => {
    setIsLoading(true)
    try {
      const token = await getAccessToken()
      const request = buildEventsRequest(false)
      const fetchedEventData =
        await EventsApiService.fetchFilteredEvents(request, token)
      setEventData(fetchedEventData)

      // If current page is beyond available pages, go to last page
      if (
        fetchedEventData.totalPages > 0 &&
        currentPage > fetchedEventData.totalPages
      ) {
        setCurrentPage(fetchedEventData.totalPages)
      }
    } catch (err) {
      console.error('Failed to fetch filtered events:', err)
    } finally {
      setIsLoading(false)
    }
  }

  const refreshAllData = async () => {
    setIsRefreshing(true)
    try {
      setCurrentPage(1)
      const token = await getAccessToken()
      const selectedColumnNames = selectedColumns.map((col) => col.name)
      const request = { ...buildEventsRequest(true), page: 1 }

      const [
        fetchedEventData,
        fetchedTimeline,
        fetchedColumns,
        fetchedColumnStats,
      ] = await Promise.all([
        EventsApiService.fetchFilteredEvents(request, token),
        EventsApiService.fetchTimeline(EventsApiService.generateCombinedTimeline(), token),
        EventsApiService.fetchColumns(token),
        EventsApiService.fetchColumnStats(selectedColumnNames, token),
      ])

      const { minutes, days } = EventsApiService.splitTimelineResponse(fetchedTimeline)

      setEventData(fetchedEventData)
      setMinutesTimelineData(minutes)
      setDaysTimelineData(days)
      setAvailableColumnNames(fetchedColumns)
      setColumnStats(fetchedColumnStats)
      setLastRefreshTime(new Date())
    } catch (err) {
      console.error('Failed to refresh data:', err)
    } finally {
      setIsRefreshing(false)
    }
  }

  const resetEvents = async () => {
    const confirmed = confirm(
      'Are you sure you want to delete all events? This action cannot be undone.'
    )
    if (!confirmed) return

    setIsRefreshing(true)
    try {
      const token = await getAccessToken()
      await EventsApiService.resetEvents(token)
      setEventData({ events: [], totalPages: 0, totalItems: 0 })
      setMinutesTimelineData({ points: [] })
      setDaysTimelineData({ points: [] })
      setAvailableColumnNames([])
      setSelectedTimeBucket(null)
      setCurrentPage(1)
      setLastRefreshTime(null)
    } catch (err) {
      console.error('Failed to reset events:', err)
    } finally {
      setIsRefreshing(false)
    }
  }

  const closeJsonPanel = () => {
    setSelectedCellId(null)
    setSelectedRowId(null)
    setJsonPanelData(null)
  }

  const openJsonPanel = (cellId: string, value: any, title: string) => {
    setSelectedCellId(cellId)
    setSelectedRowId(null)
    setJsonPanelData({ value, title })

    setTimeout(() => {
      const selectedElement = document.querySelector(
        `[data-cell-clickable="true"].bg-gray-200`
      )
      if (selectedElement) {
        selectedElement.scrollIntoView({
          behavior: 'smooth',
          block: 'nearest',
          inline: 'nearest',
        })
      }
    }, 100)
  }

  const toggleJsonPanel = (cellId: string, value: any, title: string) => {
    if (selectedCellId === cellId) {
      closeJsonPanel()
    } else {
      openJsonPanel(cellId, value, title)
    }
  }

  const handleRowClick = (rowId: string, event: Event) => {
    if (selectedRowId === rowId) {
      closeJsonPanel()
    } else {
      setSelectedRowId(rowId)
      setSelectedCellId(null)
      setJsonPanelData({ value: event, title: 'Full event' })
    }
  }

  // Check if any filters are active
  const hasActiveFilters = selectedTimeBucket !== null || columnFilters.length > 0

  // Reset all filters
  const resetAllFilters = () => {
    setSelectedTimeBucket(null)
    setColumnFilters([])
  }

  // Get active filters for tooltip
  const getActiveFilters = () => {
    const filters: string[] = []

    if (selectedTimeBucket) {
      const bucketStart = selectedTimeBucket.split('|')[0]
      const bucketEnd = selectedTimeBucket.split('|')[1]
      const startDate = new Date(bucketStart)
      const endDate = new Date(bucketEnd)
      filters.push(`Time: ${startDate.toLocaleString()} – ${endDate.toLocaleString()}`)
    }

    if (columnFilters.length > 0) {
      columnFilters.forEach((filter) => {
        filters.push(`${filter.id}: ${filter.value}`)
      })
    }

    return filters
  }

  // Debounced filter/sort/page changes
  useEffect(() => {
    if (!lastRefreshTime || !isAuthenticated) return

    const timeoutId = setTimeout(() => {
      fetchEventsWithFilters()
    }, 400)

    return () => clearTimeout(timeoutId)
  }, [columnFilters, selectedTimeBucket, currentPage, sorting, isAuthenticated])

  // Initial load - only when authentication is ready
  useEffect(() => {
    if (isAuthenticated && !authIsLoading) {
      refreshAllData()
    }
  }, [isAuthenticated, authIsLoading])

  // Show loading screen while auth is initializing
  if (authIsLoading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4" />
          <p>Loading...</p>
        </div>
      </div>
    )
  }

  // Show error screen if auth failed
  if (error) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <p className="text-red-600 mb-4">Authentication Error: {error}</p>
          <Button onClick={() => window.location.reload()}>Retry</Button>
        </div>
      </div>
    )
  }

  // Only render the app when authenticated
  if (!isAuthenticated) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <p>Redirecting to login...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="flex flex-col h-screen min-w-0 bg-page-background">
      {/* Header */}
      <div className="border-b bg-background p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center">
            <img
              src="/micro/ui/logo.svg"
              alt="Snowplow Micro"
              className="h-6"
            />
          </div>
          <div className="flex items-center gap-2">
            {lastRefreshTime && (
              <span className="text-xs text-last-refreshed font-light">
                Last refreshed at {lastRefreshTime.toLocaleTimeString()}
              </span>
            )}
            <Button
              variant="outline"
              size="sm"
              onClick={() => refreshAllData()}
              disabled={isRefreshing}
            >
              <RefreshCw
                className={`mr-2 h-4 w-4 ${isRefreshing ? 'animate-spin' : ''}`}
              />
              Refresh
            </Button>
            {hasActiveFilters && (
              <TooltipProvider>
                <Tooltip delayDuration={0}>
                  <TooltipTrigger asChild>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={resetAllFilters}
                    >
                      <FilterX className="mr-2 h-4 w-4" />
                      Reset filters
                    </Button>
                  </TooltipTrigger>
                  <TooltipContent>
                    <div>
                      <div className="font-medium">Active filters:</div>
                      {getActiveFilters().map((filter, index) => (
                        <div key={index} className="text-xs">
                          • {filter}
                        </div>
                      ))}
                    </div>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            )}
            <Button
              variant={showColumnSelector ? 'outlineActive' : 'outline'}
              size="sm"
              onClick={() => setShowColumnSelector(!showColumnSelector)}
            >
              <Columns3Cog className="mr-2 h-4 w-4" />
              Pick columns
            </Button>
            <div className="relative">
              <Button
                variant="outline"
                size="sm"
                onClick={(e) => {
                  e.preventDefault()
                  setShowActionsMenu(!showActionsMenu)
                }}
                onBlur={() => setShowActionsMenu(false)}
              >
                <MoreVertical className="h-4 w-4" />
              </Button>

              {showActionsMenu && (
                <div className="absolute top-full right-0 z-50 mt-1 bg-white border rounded-md shadow-lg whitespace-nowrap">
                  <div className="p-1">
                    <button
                      className="px-2 py-1 text-xs font-normal text-left hover:bg-gray-100 rounded-sm flex items-center gap-2"
                      onMouseDown={(e) => {
                        e.preventDefault() // Prevent button blur
                        resetEvents()
                        setShowActionsMenu(false)
                      }}
                    >
                      <Trash2 className="h-4 w-4 text-failure" />
                      Delete all events
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>

      <div className="flex flex-1 overflow-hidden">
        {/* Main Content */}
        <div className="h-full p-4 min-w-0 flex flex-1 flex-col gap-4">
          {/* Timeline Charts */}
          <div className="flex gap-4">
            <div className="flex-1">
              <EventsChart
                timelineData={daysTimelineData}
                selectedBucket={selectedTimeBucket}
                onBucketClick={setSelectedTimeBucket}
                timeFormat={(date) => date.toLocaleDateString('en-US', {
                  month: 'short',
                  day: 'numeric',
                  hour: '2-digit'
                })}
              />
            </div>
            <div className="flex-1">
              <EventsChart
                timelineData={minutesTimelineData}
                selectedBucket={selectedTimeBucket}
                onBucketClick={setSelectedTimeBucket}
                timeFormat={(date) => date.toLocaleTimeString('en-US', {
                  hour12: false,
                  hour: '2-digit',
                  minute: '2-digit',
                })}
              />
            </div>
          </div>

          {/* Data Table */}
          <div className="flex-1 min-h-0">
            <DataTable
              events={eventData.events}
              selectedColumns={selectedColumns}
              selectedCellId={selectedCellId}
              columnFilters={columnFilters}
              setColumnFilters={setColumnFilters}
              selectedTimeBucket={selectedTimeBucket}
              onJsonCellToggle={toggleJsonPanel}
              onReorderColumns={reorderColumns}
              onRowClick={handleRowClick}
              selectedRowId={selectedRowId}
              columnStats={columnStats}
              currentPage={currentPage}
              totalPages={eventData.totalPages}
              totalItems={eventData.totalItems}
              onPageChange={setCurrentPage}
              sorting={sorting}
              onSortingChange={setSorting}
              isLoading={isLoading}
            />
          </div>
        </div>

        {/* JSON Side Panel */}
        {jsonPanelData && (
          <JsonSidePanel
            value={jsonPanelData.value}
            title={jsonPanelData.title}
            onClose={closeJsonPanel}
            maxDepth={
              jsonPanelData.title === 'Full event'
                ? 1
                : jsonPanelData.title === 'Failures'
                  ? 4
                  : undefined
            }
          />
        )}

        {/* Column Selector Sidebar */}
        {showColumnSelector && (
          <div className="w-80 min-w-[200px] flex-shrink-0">
            <ColumnSelector
              availableColumns={availableColumns}
              selectedColumns={selectedColumns}
              onToggleColumn={toggleColumn}
              onClose={() => setShowColumnSelector(false)}
              onResetToDefaults={resetToDefaults}
            />
          </div>
        )}
      </div>
    </div>
  )
}

export default App
