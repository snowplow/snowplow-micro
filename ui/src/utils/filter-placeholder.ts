const formatCount = (n: number): string => {
  if (n >= 1_000_000 && n % 1_000_000 === 0) return `${n / 1_000_000}M`
  if (n >= 1_000 && n % 1_000 === 0) return `${n / 1_000}k`
  return n.toLocaleString('en-US')
}

/**
 * Placeholder of a column's filter. When the backend only searches the most
 * recent `searchLimit` events for this column, the placeholder says so.
 */
export const filterPlaceholder = (searchLimit?: number | null): string =>
  searchLimit === undefined || searchLimit === null
    ? 'Filter...'
    : `Filter (last ${formatCount(searchLimit)} events)...`
