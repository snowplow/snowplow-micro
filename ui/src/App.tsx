import { useState, useEffect } from 'react'
import { DataTable } from '@/components/DataTable'
import { ColumnSelector } from '@/components/ColumnSelector'
import { JsonSidePanel } from '@/components/JsonSidePanel'
import { EventsChart } from '@/components/EventsChart'
import { type Event, EventsApiService } from '@/services/api'
import { useColumnManager } from '@/hooks/useColumnManager'
import { Button } from '@/components/ui/button'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { RefreshCw, Trash2, Columns3Cog, FilterX, MoreVertical } from 'lucide-react'
import { type ColumnFiltersState } from '@tanstack/react-table'
import { roundToMinute } from '@/utils/event-utils'

function App() {
  const [events, setEvents] = useState<Event[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [showColumnSelector, setShowColumnSelector] = useState(false)
  const [selectedCellId, setSelectedCellId] = useState<string | null>(null)
  const [selectedRowId, setSelectedRowId] = useState<string | null>(null)
  const [jsonPanelData, setJsonPanelData] = useState<{
    value: any
    title: string
  } | null>(null)
  const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
  const [selectedMinute, setSelectedMinute] = useState<string | null>(null)
  const [lastRefreshTime, setLastRefreshTime] = useState<Date | null>(null)
  const [showActionsMenu, setShowActionsMenu] = useState(false)

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

  const { availableColumns, selectedColumns, toggleColumn, reorderColumns, resetToDefaults } =
    useColumnManager({ events, setColumnFilters, onColumnAdded: scrollToLastColumn })

  // Filter events based on selected minute
  const filteredEvents = events.filter((event) => {
    // Filter by selected minute
    if (selectedMinute && event.collector_tstamp) {
      const eventTime = new Date(event.collector_tstamp).getTime()
      const selectedTime = new Date(selectedMinute).getTime()
      const minuteStart = roundToMinute(eventTime)

      if (minuteStart !== selectedTime) return false
    }

    return true
  })

  const fetchEvents = async () => {
    setIsLoading(true)

    try {
      const fetchedEvents = await EventsApiService.fetchEvents()
      setEvents(fetchedEvents)
      setLastRefreshTime(new Date())
    } catch (err) {
      console.error('Failed to fetch events:', err)
    } finally {
      setIsLoading(false)
    }
  }

  const resetEvents = async () => {
    const confirmed = confirm(
      'Are you sure you want to delete all events? This action cannot be undone.'
    )
    if (!confirmed) return

    setIsLoading(true)
    try {
      await EventsApiService.resetEvents()
      setEvents([])
      setSelectedMinute(null)
    } catch (err) {
      console.error('Failed to reset events:', err)
    } finally {
      setIsLoading(false)
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
  const hasActiveFilters =
    selectedMinute !== null || columnFilters.length > 0

  // Reset all filters
  const resetAllFilters = () => {
    setSelectedMinute(null)
    setColumnFilters([])
  }

  // Get active filters for tooltip
  const getActiveFilters = () => {
    const filters: string[] = []

    if (selectedMinute) {
      const date = new Date(selectedMinute)
      filters.push(`Time: ${date.toLocaleTimeString()}`)
    }

    if (columnFilters.length > 0) {
      columnFilters.forEach((filter) => {
        filters.push(`${filter.id}: ${filter.value}`)
      })
    }

    return filters
  }

  // Initial load
  useEffect(() => {
    fetchEvents()
  }, [])

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
              onClick={() => fetchEvents()}
              disabled={isLoading}
            >
              <RefreshCw
                className={`mr-2 h-4 w-4 ${isLoading ? 'animate-spin' : ''}`}
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
          {/* Events Chart */}
          <EventsChart
            events={events}
            selectedMinute={selectedMinute}
            onMinuteClick={setSelectedMinute}
          />

          {/* Data Table */}
          <div className="flex-1 min-h-0">
            <DataTable
              events={filteredEvents}
              selectedColumns={selectedColumns}
              selectedCellId={selectedCellId}
              columnFilters={columnFilters}
              setColumnFilters={setColumnFilters}
              selectedMinute={selectedMinute}
              onJsonCellToggle={toggleJsonPanel}
              onReorderColumns={reorderColumns}
              onRowClick={handleRowClick}
              selectedRowId={selectedRowId}
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
