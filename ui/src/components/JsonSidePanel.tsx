import { useState, useEffect, useCallback } from 'react'
import { X } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { JsonViewer } from '@/components/JsonViewer'
import { TooltipProvider } from '@/components/ui/tooltip'
import { TruncatedColumnName } from '@/components/TruncatedColumnName'
import { CopyButton } from '@/components/CopyButton'
import { createColumnMetadata } from '@/utils/column-metadata'

const STORAGE_KEY = 'snowplow-micro-json-panel-width'
const DEFAULT_WIDTH = 400
const MIN_WIDTH = 300
const MAX_WIDTH = 800

/**
 * Save panel width to localStorage
 */
const saveWidth = (width: number) => {
  try {
    localStorage.setItem(STORAGE_KEY, width.toString())
  } catch (error) {
    console.warn('Failed to save panel width to localStorage:', error)
  }
}

/**
 * Load panel width from localStorage
 */
const loadWidth = (): number => {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      const width = parseInt(stored, 10)
      if (!isNaN(width) && width >= MIN_WIDTH && width <= MAX_WIDTH) {
        return width
      }
    }
  } catch (error) {
    console.warn('Failed to load panel width from localStorage:', error)
  }
  return DEFAULT_WIDTH
}

type JsonSidePanelProps = {
  value: any
  title: string
  onClose: () => void
  maxDepth?: number
}

export function JsonSidePanel({
  value,
  title,
  onClose,
  maxDepth,
}: JsonSidePanelProps) {
  const [width, setWidth] = useState(loadWidth())
  const [isResizing, setIsResizing] = useState(false)

  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    setIsResizing(true)
  }, [])

  const handleMouseMove = useCallback((e: MouseEvent) => {
    if (!isResizing) return

    const newWidth = window.innerWidth - e.clientX
    const clampedWidth = Math.max(MIN_WIDTH, Math.min(MAX_WIDTH, newWidth))
    setWidth(clampedWidth)
  }, [isResizing])

  const handleMouseUp = useCallback(() => {
    if (isResizing) {
      setIsResizing(false)
      saveWidth(width)
    }
  }, [isResizing, width])

  // Add global mouse event listeners during resize
  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleMouseMove)
      document.addEventListener('mouseup', handleMouseUp)
      document.body.style.cursor = 'col-resize'
      document.body.style.userSelect = 'none'

      return () => {
        document.removeEventListener('mousemove', handleMouseMove)
        document.removeEventListener('mouseup', handleMouseUp)
        document.body.style.cursor = ''
        document.body.style.userSelect = ''
      }
    }
  }, [isResizing, handleMouseMove, handleMouseUp])

  return (
    <TooltipProvider>
      <div
        className="flex-shrink-0 bg-background h-full overflow-hidden flex flex-col p-4 gap-4 relative"
        style={{ width: `${width}px`, minWidth: `${MIN_WIDTH}px`, maxWidth: `${MAX_WIDTH}px` }}
      >
        {/* Resize handle */}
        <div
          className={`absolute left-0 top-0 w-1 h-full cursor-col-resize border-l-1 hover:border-l-2 hover:border-focus! ${
            isResizing ? 'border-l-2 border-focus!' : ''
          }`}
          onMouseDown={handleMouseDown}
        />

        {/* Header with title and close button */}
        <div className="bg-background">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <TruncatedColumnName
                columnMetadata={createColumnMetadata(title)}
                iconSize="md"
                className="text-lg font-light truncate"
              />
              <CopyButton value={value} alwaysVisible />
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              className="h-8 w-8 p-0"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>

        {/* JSON content */}
        <div className="flex-1 overflow-y-auto -ml-2">
          <JsonViewer data={value} maxDepth={maxDepth} />
        </div>
      </div>
    </TooltipProvider>
  )
}
