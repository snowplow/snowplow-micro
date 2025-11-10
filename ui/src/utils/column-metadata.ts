import { truncateMiddle } from './event-utils'
import type { Event } from '@/services/api'

export type ColumnMetadata = {
  name: string // original name (e.g., "contexts_schema.field")
  isNested: boolean
  isJSON: boolean
  isTimestamp: boolean
  parentColumn?: string
  fieldName?: string
  truncatedName?: string // only present if truncated
  icon: 'entity' | 'event' | null
  accessor: (event: Event) => any // function to get the value from an event
}

/**
 * Create metadata for a single column
 */
export function createColumnMetadata(columnName: string): ColumnMetadata {
  const isNested = columnName.includes('.')
  const isJSON =
    columnName.startsWith('unstruct_event_') ||
    columnName.startsWith('contexts_')
  const isTimestamp = columnName.endsWith('_tstamp')

  // Compute display text and icon (same for both nested and non-nested)
  let displayText = columnName
  let icon: 'entity' | 'event' | null = null
  let wasTruncated = false

  // Check for contexts_ prefix (removing prefix counts as truncation)
  if (displayText.startsWith('contexts_')) {
    displayText = displayText
      .replace('contexts_', '')
      .replace('com_snowplowanalytics_snowplow_', '')
    icon = 'entity'
    wasTruncated = true
  }

  // Check for unstruct_event_ prefix (removing prefix counts as truncation)
  if (displayText.startsWith('unstruct_event_')) {
    displayText = displayText
      .replace('unstruct_event_', '')
      .replace('com_snowplowanalytics_snowplow_', '')
    icon = 'event'
    wasTruncated = true
  }

  // Additional truncation if still too long
  if (displayText.length > 35) {
    displayText = truncateMiddle(displayText)
    wasTruncated = true
  }

  if (!isNested) {
    // Handle non-nested columns
    const accessor = (event: Event): any => event[columnName]

    return {
      name: columnName,
      isNested: false,
      isJSON,
      isTimestamp,
      parentColumn: undefined,
      fieldName: undefined,
      truncatedName: wasTruncated ? displayText : undefined,
      icon,
      accessor,
    }
  } else {
    // Handle nested columns
    const parentColumn = columnName.substring(0, columnName.indexOf('.'))
    const fieldName = columnName.substring(columnName.indexOf('.') + 1)

    const accessor = (event: Event): any => {
      const parentValue = event[parentColumn]

      if (parentValue === undefined || parentValue === null) {
        return undefined
      }

      try {
        if (Array.isArray(parentValue)) {
          // For contexts_ arrays, extract the field from each object
          const results: any[] = []
          parentValue.forEach((item) => {
            if (item && typeof item === 'object' && fieldName in item) {
              results.push(item[fieldName])
            }
          })
          return results.length > 0 ? results : undefined
        } else if (typeof parentValue === 'object') {
          // For unstruct_event_ objects, extract the field directly
          return parentValue[fieldName]
        }
      } catch (error) {
        console.warn(`Failed to extract field ${columnName}:`, error)
      }

      return undefined
    }

    return {
      name: columnName,
      isNested: true,
      isJSON,
      isTimestamp,
      parentColumn,
      fieldName,
      truncatedName: wasTruncated ? displayText : undefined,
      icon,
      accessor,
    }
  }
}
