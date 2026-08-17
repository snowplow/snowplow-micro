export const TIMESTAMP_COLUMN = 'collector_tstamp'
export const FAILURE_COLUMN = 'contexts_com_snowplowanalytics_snowplow_failure_1'

const FIXED_COLUMNS = [TIMESTAMP_COLUMN, FAILURE_COLUMN]

export const isFixedColumn = (columnName: string): boolean =>
  FIXED_COLUMNS.some(
    (fixed) => columnName === fixed || columnName.startsWith(`${fixed}.`)
  )
