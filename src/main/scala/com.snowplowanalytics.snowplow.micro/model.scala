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
