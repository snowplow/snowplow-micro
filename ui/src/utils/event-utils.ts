import type { Event } from '@/services/api'

/**
 * Check if an event has failure data
 */
export function hasFailureData(event: Event): boolean {
  const failureData = event.contexts_com_snowplowanalytics_snowplow_failure_1
  return (
    failureData !== undefined &&
    failureData !== null &&
    Array.isArray(failureData) &&
    failureData.length > 0
  )
}

/**
 * Truncate text in the middle with ellipsis
 */
export function truncateMiddle(text: string, maxLength: number = 35): string {
  if (text.length <= maxLength) {
    return text
  }

  const ellipsis = '[...]'
  const availableLength = maxLength - ellipsis.length
  const startLength = Math.ceil(availableLength / 2)
  const endLength = Math.floor(availableLength / 2)

  const start = text.substring(0, startLength)
  const end = text.substring(text.length - endLength)

  return `${start}${ellipsis}${end}`
}
