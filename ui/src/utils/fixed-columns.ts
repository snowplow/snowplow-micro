// The event fields behind the two fixed columns: Status is derived from the failure
// entity, and the timestamp column carries the time filter. Neither is offered in the
// column picker; selections persisted or shared before they became fixed may still
// mention them, and those references are ignored.
export const TIMESTAMP_COLUMN = 'collector_tstamp'
export const FAILURE_COLUMN = 'contexts_com_snowplowanalytics_snowplow_failure_1'

const FIXED_COLUMNS = [TIMESTAMP_COLUMN, FAILURE_COLUMN]

// A nested field of a fixed column (`<name>.<field>`) is fixed along with it
export const isFixedColumn = (columnName: string): boolean =>
  FIXED_COLUMNS.some(
    (fixed) => columnName === fixed || columnName.startsWith(`${fixed}.`)
  )
