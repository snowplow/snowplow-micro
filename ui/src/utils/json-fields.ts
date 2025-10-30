/**
 * Convert a value to a searchable JSON string for filtering
 */
export function valueToSearchableString(value: any): string {
  if (value === undefined || value === null) {
    return ''
  }

  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

/**
 * Truncate JSON string for display (preserve readability)
 */
export function truncateJsonForDisplay(
  value: any,
  columnName: string,
  maxLength: number = 100
): string {
  // For arrays in contexts columns, show elements one per line
  if (Array.isArray(value) && columnName.startsWith('contexts_')) {
    const lines = value.map((item) => JSON.stringify(item))
    const result = lines.join('\n')

    if (result.length <= maxLength) {
      return result
    }

    // If too long, truncate by including as many complete lines as possible
    let truncated = ''
    let currentLength = 0

    for (const line of lines) {
      if (currentLength + line.length + 1 > maxLength) {
        // +1 for newline
        if (truncated === '') {
          // If even the first line is too long, truncate it
          return line.substring(0, maxLength - 3) + '...'
        }
        break
      }
      if (truncated !== '') truncated += '\n'
      truncated += line
      currentLength = truncated.length
    }

    return truncated + '\n...'
  }

  // For other values, use existing logic
  const jsonString = valueToSearchableString(value)

  if (jsonString.length <= maxLength) {
    return jsonString
  }

  // Try to truncate at a reasonable point (after a comma or closing bracket/brace)
  const truncated = jsonString.substring(0, maxLength)
  const lastComma = truncated.lastIndexOf(',')
  const lastBracket = Math.max(
    truncated.lastIndexOf('}'),
    truncated.lastIndexOf(']')
  )

  const cutPoint = Math.max(lastComma, lastBracket)
  if (cutPoint > maxLength * 0.7) {
    return truncated.substring(0, cutPoint + 1) + '...'
  }

  return truncated + '...'
}
