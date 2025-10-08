import { useState, useEffect, useRef } from 'react'
import { Search, X, RotateCcw } from 'lucide-react'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { TooltipProvider } from '@/components/ui/tooltip'
import { TruncatedColumnName } from '@/components/TruncatedColumnName'
import { type ColumnMetadata } from '@/utils/column-metadata'

type ColumnSelectorProps = {
  availableColumns: ColumnMetadata[]
  selectedColumns: ColumnMetadata[]
  onToggleColumn: (fieldName: string) => void
  onClose: () => void
  onResetToDefaults: () => void
}

export function ColumnSelector({
  availableColumns,
  selectedColumns,
  onToggleColumn,
  onClose,
  onResetToDefaults,
}: ColumnSelectorProps) {
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedTab, setSelectedTab] = useState<
    'selected' | 'atomic' | 'events' | 'entities'
  >('selected')
  const [everSelected] = useState<Set<string>>(new Set(selectedColumns.map(col => col.name)))
  const searchInputRef = useRef<HTMLInputElement>(null)

  // Auto-focus search input when component mounts or tab changes
  useEffect(() => {
    if (searchInputRef.current) {
      searchInputRef.current.focus()
    }
  }, [selectedTab])

  // Keep once selected columns in the selected tab so that they don't disappear when unselected
  const toggleAndKeep = (name: string) => {
    everSelected.add(name)
    onToggleColumn(name)
  }

  const selected = new Set(selectedColumns.map(col => col.name))

  const filtered = availableColumns
    .filter((columnMetadata) => {
      const { name } = columnMetadata
      // Handle "Selected" tab - show initially selected columns OR currently selected columns
      if (selectedTab === 'selected') {
        const matchesSelection = everSelected.has(name) || selected.has(name)
        if (!matchesSelection) return false

        // Filter by search term - search the full column name for selected tab
        if (searchTerm) {
          return name.toLowerCase().includes(searchTerm.toLowerCase())
        }
        return true
      }

      // Get the parent column name for nested fields
      const parentColumn = columnMetadata.isNested ? columnMetadata.parentColumn! : name

      // First filter by tab based on parent column
      let matchesTab = false
      if (selectedTab === 'events') {
        matchesTab = parentColumn.startsWith('unstruct_event_')
      } else if (selectedTab === 'entities') {
        matchesTab = parentColumn.startsWith('contexts_')
      } else {
        // atomic
        matchesTab =
          !parentColumn.startsWith('unstruct_event_') &&
          !parentColumn.startsWith('contexts_')
      }

      if (!matchesTab) return false

      // Then filter by search term - only search the parent column name
      if (searchTerm) {
        return parentColumn.toLowerCase().includes(searchTerm.toLowerCase())
      }

      return true
    })
    .sort((a, b) => a.name.localeCompare(b.name))

  return (
    <TooltipProvider>
      <div className="border-l bg-background h-full overflow-hidden flex flex-col p-4 gap-4">
        {/* Header with title and close button */}
          <div className="flex items-center justify-between">
          <h2 className="text-lg font-light truncate">Columns</h2>
          <Button
            variant="ghost"
            size="sm"
            onClick={onClose}
            className="h-8 w-8 p-0"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>

        <div className="flex-shrink-0">
          <div className="space-y-3">
            {/* Toggle button group */}
            <ToggleGroup
              type="single"
              value={selectedTab}
              onValueChange={(value) =>
                value &&
                setSelectedTab(
                  value as 'selected' | 'atomic' | 'events' | 'entities'
                )
              }
              variant="outline"
              size="sm"
              className="w-full"
            >
              <ToggleGroupItem value="selected" className="flex-1">
                Selected
              </ToggleGroupItem>
              <ToggleGroupItem value="atomic" className="flex-1">
                Atomic
              </ToggleGroupItem>
              <ToggleGroupItem value="events" className="flex-1">
                Events
              </ToggleGroupItem>
              <ToggleGroupItem value="entities" className="flex-1">
                Entities
              </ToggleGroupItem>
            </ToggleGroup>

            {/* Search input */}
            <div className="relative">
              <Search className="absolute left-2 top-2 h-4 w-4 text-muted-foreground" />
              <Input
                ref={searchInputRef}
                placeholder="Search columns..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-8 font-light"
              />
            </div>
          </div>
        </div>

        {/* Column list */}
        <div className="flex-1 overflow-y-auto">
          {filtered.length === 0 ? (
            <div className="text-center text-muted-foreground py-8 text-sm font-light">
              {searchTerm
                ? `No columns matching "${searchTerm}"`
                : 'No columns'}
            </div>
          ) : (
            filtered.map((columnMetadata) => {
              const { name, isNested, fieldName } = columnMetadata
              // for Selected tab, always show full name and icon
              // for other tabs, show field name only for nested columns and no icon
              const updatedColumnMetadata = selectedTab !== 'selected' && isNested ?
                {...columnMetadata, icon: null, truncatedName: fieldName} : columnMetadata

              // for Selected tab, don't indent nested fields
              const shouldIndent = selectedTab !== 'selected' && isNested

              return (
                <div
                  key={name}
                  className={`flex items-center gap-3 p-1 rounded hover:bg-muted/50 cursor-pointer transition-colors ${
                    shouldIndent ? 'ml-6' : ''
                  }`}
                  onClick={() => toggleAndKeep(name)}
                >
                  {/* Checkbox on the left */}
                  <Checkbox
                    checked={selected.has(name)}
                    onCheckedChange={() => toggleAndKeep(name)}
                    onClick={(e) => e.stopPropagation()}
                  />

                  {/* Field name with truncation and tooltip */}
                  <div className="flex-1 min-w-0">
                    <TruncatedColumnName
                      columnMetadata={updatedColumnMetadata}
                      className="text-sm font-light"
                    />
                  </div>
                </div>
              )
            })
          )}
        </div>

        {/* Reset button for selected tab */}
        {selectedTab === 'selected' && (
          <Button
            variant="outline"
            size="sm"
            onClick={onResetToDefaults}
            className="w-full mt-3 border-t flex items-center gap-2"
          >
            <RotateCcw className="h-4 w-4" />
            Reset
          </Button>
        )}
      </div>
    </TooltipProvider>
  )
}
