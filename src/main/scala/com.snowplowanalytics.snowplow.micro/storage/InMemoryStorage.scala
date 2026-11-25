/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.micro.storage

import cats.effect.IO
import com.snowplowanalytics.snowplow.micro._
import com.snowplowanalytics.snowplow.micro.model.ColumnStatsResponse
import io.circe.Json


/** In-memory cache containing the results of the validation (or not) of the tracking events.
  * Good events are stored with their type, their schema and their contexts, if any,
  * so that they can be quickly filtered.
  * Bad events are stored with the error message(s) describing what when wrong.
  */
private[micro] class InMemoryStorage extends EventStorage {
  import InMemoryStorage._

  protected var good = List.empty[GoodEvent]
  private object LockGood
  protected var bad = List.empty[BadEvent]
  private object LockBad

  /** Add a good event to the cache. */
  override def addToGood(events: List[GoodEvent]): IO[Unit] = IO.delay {
    LockGood.synchronized {
      good = events ++ good
    }
  }

  /** Add a bad event to the cache. */
  override def addToBad(events: List[BadEvent]): IO[Unit] = IO.delay {
    LockBad.synchronized {
      bad = events ++ bad
    }
  }

  /** Remove all the events from memory. */
  override def reset(): IO[Unit] = IO.delay {
    LockGood.synchronized {
      good = List.empty[GoodEvent]
    }
    LockBad.synchronized {
      bad = List.empty[BadEvent]
    }
  }

  /** Compute a summary with the number of good and bad events currently in cache. */
  def getSummary(): ValidationSummary = {
    val nbGood = LockGood.synchronized {
      good.filterNot(_.incomplete).size
    }
    val nbBad = LockBad.synchronized {
      bad.size
    }
    ValidationSummary(nbGood + nbBad, nbGood, nbBad)
  }

  /** Filter out the good events with the possible filters contained in the HTTP request. */
  def filterGood(
    filtersGood: FiltersGood = FiltersGood(None, None, None, None)
  ): List[GoodEvent] =
    LockGood.synchronized {
      val filtered = good.filterNot(_.incomplete).filter(keepGoodEvent(_, filtersGood))
      filtered.take(filtersGood.limit.getOrElse(filtered.size))
    }

  /** Filter out the bad events with the possible filters contained in the HTTP request. */
  def filterBad(
    filtersBad: FiltersBad = FiltersBad(None, None, None)
  ): List[BadEvent] =
    LockBad.synchronized {
      val filtered = bad.filter(keepBadEvent(_, filtersBad))
      filtered.take(filtersBad.limit.getOrElse(filtered.size))
    }

  /** Get all good + incomplete events */
  private def getGoodAndIncomplete: List[GoodEvent] =
    LockGood.synchronized(good)

  private def getEvents: IO[List[Json]] = IO.delay {
    getGoodAndIncomplete.map(_.event.toJson(lossy = true))
  }

  override def getColumns: IO[List[String]] = {
    getEvents.map { jsonEvents =>
      jsonEvents.map(EventStorage.extractColumnsFromEvent)
        .fold(Set.empty[String])(_.union(_))
        .toList
        .sorted
    }
  }

  override def getTimeline(request: TimelineRequest): IO[TimelineData] = IO.delay {
    val allEvents = getGoodAndIncomplete

    val points = request.buckets.map { bucket =>
      val eventsInBucket = allEvents.filter { event =>
        val timestamp = event.event.collector_tstamp
        !timestamp.isBefore(bucket.start) && timestamp.isBefore(bucket.end)
      }

      val (failed, valid) = eventsInBucket.partition(_.incomplete)
      TimelinePoint(valid.size, failed.size, bucket)
    }

    TimelineData(points)
  }

  override def getColumnStats(columns: List[String]): IO[ColumnStatsResponse] = {
    getEvents.map { jsonEvents =>
      columns.map { column =>
        // Sorting on fields of complex columns isn't supported
        val sortable = !EventStorage.isComplexColumn(column)

        val filterable = EventStorage.filterableColumn(column).isDefined

        val distinctValues = if (filterable) {
          val values = jsonEvents.iterator.flatMap(InMemoryStorage.getColumnValues(_, column)).distinct.take(20).toList
          Some(values)
        } else {
          None
        }

        column -> ColumnStats(
          sortable = sortable,
          filterable = filterable,
          values = distinctValues
        )
      }.toMap
    }
  }

  override def getFilteredEvents(request: EventsRequest): IO[EventsResponse] = {
    IO.delay {
      val events = getGoodAndIncomplete

      val preFilteredEvents = events.filter { event =>
        request.validEvents.forall { validOnly =>
          validOnly == !event.incomplete
        } &&
        request.timeRange.forall { timeRange =>
          applyTimeFilter(event, timeRange)
        }
      }

      val filteredEvents = preFilteredEvents
        .map(_.event.toJson(lossy = true))
        .filter { eventJson =>
          request.filters.filter(f => EventStorage.filterableColumn(f.column).isDefined).forall { filter =>
            applyFilter(eventJson, filter)
          }
        }

      val sortedEvents = request.sorting match {
        case Some(sorting) if !EventStorage.isComplexColumn(sorting.column) =>
          applySorting(filteredEvents, sorting)
        case _ => filteredEvents
      }

      val totalItems = sortedEvents.size
      val totalPages = Math.max(1, (totalItems + request.pageSize - 1) / Math.max(request.pageSize, 1))
      val startIndex = (request.page - 1) * request.pageSize
      val endIndex = Math.min(startIndex + request.pageSize, totalItems)

      val pageEvents = if (startIndex < totalItems) {
        sortedEvents.slice(startIndex, endIndex)
      } else {
        List.empty
      }

      EventsResponse(pageEvents, totalPages, totalItems)
    }
  }
}

private[micro] object InMemoryStorage {
  /** Check if a good event matches the filters. */
  private[micro] def keepGoodEvent(event: GoodEvent, filters: FiltersGood): Boolean =
    filters.event_type.toSet.subsetOf(event.eventType.toSet) &&
      filters.schema.toSet.subsetOf(event.schema.toSet) &&
      filters.contexts.forall(containsAllContexts(event, _))

  /** Check if an event contains all the contexts of the list. */
  private[micro] def containsAllContexts(event: GoodEvent, contexts: List[String]): Boolean =
    contexts.forall(event.contexts.contains)

  /** Check if a bad event matches the filters. */
  private[micro] def keepBadEvent(event: BadEvent, filters: FiltersBad): Boolean =
    filters.vendor.forall(vendor => event.collectorPayload.forall(_ .api.vendor == vendor)) &&
      filters.version.forall(version => event.collectorPayload.forall(_ .api.version == version))

  /* --- Filters and sorting for the newer /micro/events endpoint */

  /** Value of a column that holds a single value: a standard column or a field of the self-describing event.
    * Entity fields (`contexts_x.field`) can hold several values, see [[getColumnValues]].
    */
  private[micro] def getColumnValue(eventJson: Json, column: String): Option[String] = {
    if (EventStorage.isComplexColumn(column)) {
      EventStorage.filterableColumn(column).collect {
        case EventStorage.FilterableColumn.UnstructEventField(parent, field) =>
          eventJson.hcursor.downField(parent).downField(field).focus.flatMap(nestedValueAsString)
      }.flatten
    } else {
      // Handle simple columns
      eventJson.asObject
        .flatMap(_.apply(column))
        .map(valueAsString)
    }
  }

  /** All the values of a column in an event. For an entity field (`contexts_x.field`),
    * the values of that field in every entity of that type.
    */
  private[micro] def getColumnValues(eventJson: Json, column: String): List[String] =
    EventStorage.filterableColumn(column) match {
      case Some(EventStorage.FilterableColumn.ContextsField(parent, field)) =>
        eventJson.hcursor.downField(parent).focus.flatMap(_.asArray).toList.flatten.flatMap { entity =>
          entity.hcursor.downField(field).focus.flatMap(nestedValueAsString)
        }
      case _ =>
        getColumnValue(eventJson, column).toList
    }

  private def valueAsString(value: Json): String =
    value.asString.getOrElse(value.noSpaces)

  private def nestedValueAsString(value: Json): Option[String] =
    if (value.isNull) None else Some(valueAsString(value))

  private[micro] def applyFilter(eventJson: Json, filter: EventsFilter): Boolean = {
    val filterValues = filter.values.filter(_.nonEmpty)
    if (filterValues.isEmpty) return true

    getColumnValues(eventJson, filter.column).exists { value =>
      filterValues.exists(fv => value.equalsIgnoreCase(fv))
    }
  }

  private[micro] def applyTimeFilter(goodEvent: GoodEvent, timeRange: TimeRange): Boolean = {
    val timestamp = goodEvent.event.collector_tstamp
    val afterStart = timeRange.start.forall(start => timestamp.compareTo(start) >= 0)
    val beforeEnd = timeRange.end.forall(end => timestamp.compareTo(end) < 0)
    afterStart && beforeEnd
  }

  private[micro] def applySorting(events: List[Json], sorting: EventsSorting): List[Json] = {
    val sorted = events.sortWith { (a, b) =>
      val valueA = getColumnValue(a, sorting.column)
      val valueB = getColumnValue(b, sorting.column)

      (valueA, valueB) match {
        case (Some(va), Some(vb)) =>
          if (sorting.desc) vb.compare(va) < 0 else va.compare(vb) < 0
        case (Some(_), None) => !sorting.desc // Non-null values first when ascending
        case (None, Some(_)) => sorting.desc
        case (None, None) => false
      }
    }
    sorted
  }
}
