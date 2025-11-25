/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.micro

import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import io.circe.Json

import java.time.Instant

/** A list of this case class is returned when /micro/good is queried. */
final case class GoodEvent(
  rawEvent: RawEvent,
  eventType: Option[String],
  schema: Option[String],
  contexts: List[String],
  event: Event,
  incomplete: Boolean = false
)

/** A list of this case class is returned when /micro/bad is queried. */
final case class BadEvent(
  collectorPayload: Option[CollectorPayload],
  rawEvent: Option[RawEvent],
  errors: List[String]
)

/** Format of the JSON with the filters for a request made to /micro/good. */
private [micro] final case class FiltersGood(
  event_type: Option[String],
  schema: Option[String],
  contexts: Option[List[String]],
  limit: Option[Int]
)

/** Format of the JSON with the filters for a request made to /micro/bad. */
private [micro] final case class FiltersBad(
  vendor: Option[String],
  version: Option[String],
  limit: Option[Int]
)

/** Retuned when /micro/all is queried, and also /micro/reset. */
private [micro] final case class ValidationSummary(total: Int, good: Int, bad: Int)

/** Timeline data structures for /micro/timeline endpoint. */
final case class TimelinePoint(validEvents: Int, failedEvents: Int, timestamp: Instant)
final case class TimelineData(points: List[TimelinePoint])

/** Column statistics data structures for /micro/columnStats endpoint. */
final case class ColumnStats(values: List[String])
final case class ColumnStatsRequest(columns: List[String])
final case class ColumnStatsResponse(
  stats: Map[String, ColumnStats],
  sortableColumns: Option[List[String]]
)

/** Server-side filtering, sorting, and pagination for /micro/events endpoint. */
final case class EventsFilter(
  column: String,
  value: String
)

final case class EventsRequest(
  filters: List[EventsFilter],
  validEvents: Option[Boolean], // Some(true) = valid only, Some(false) = failed only, None = all
  timeRange: Option[TimeRange],
  sorting: Option[EventsSorting],
  page: Int,
  pageSize: Int
)

final case class TimeRange(start: Option[Instant], end: Option[Instant])

final case class EventsSorting(column: String, desc: Boolean)

final case class EventsResponse(
  events: List[Json],
  totalPages: Int,
  totalItems: Int
)
