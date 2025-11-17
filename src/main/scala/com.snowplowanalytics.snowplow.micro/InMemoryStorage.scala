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

import cats.effect.IO
import io.circe.Json

import java.time.temporal.ChronoUnit

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

  override def getTimeline: IO[TimelineData] = IO.delay {
    val groupedByMinute = LockGood.synchronized {
      good.groupBy(event => event.event.collector_tstamp.truncatedTo(ChronoUnit.MINUTES))
        .map {
          case (minute, events) =>
            val (failed, valid) = events.partition(_.incomplete)
            TimelinePoint(valid.size, failed.size, minute)
        }.toList
    }
    val filledPoints = EventStorage.fillMissingMinutes(groupedByMinute)
    TimelineData(filledPoints)
  }

  override def getColumnStats(columns: List[String]): IO[Map[String, ColumnStats]] = {
    getEvents.map { jsonEvents =>
      // TODO: support complex columns at some point
      val simpleColumns = columns.filterNot(col =>
        EventStorage.isComplexColumn(col) ||
          EventStorage.isTimestampColumn(col)
      )

      simpleColumns.flatMap { column =>
        val distinctValues = jsonEvents.flatMap { json =>
          json.asObject.flatMap(_.apply(column)).flatMap {
            case value if value.isNull => None
            case value => Some(value.asString.getOrElse(value.noSpaces))
          }
        }.distinct.take(20)

        if (distinctValues.nonEmpty) {
          Some(column -> ColumnStats(distinctValues))
        } else {
          None
        }
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
          request.filters.filterNot(f => EventStorage.isComplexColumn(f.column)).forall { filter =>
            applyFilter(eventJson, filter.column, filter.value)
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

      EventsResponse(pageEvents, totalPages, totalItems, isApproximate = false)
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

  private[micro] def getColumnValue(eventJson: Json, column: String): Option[String] = {
    if (EventStorage.isComplexColumn(column)) {
      // TODO: support complex columns
      None
    } else {
      // Handle simple columns
      eventJson.asObject
        .flatMap(_.apply(column))
        .map(value => value.asString.getOrElse(value.noSpaces))
    }
  }

  private[micro] def applyFilter(eventJson: Json, column: String, filterValue: String): Boolean = {
    if (filterValue.isEmpty) return true

    getColumnValue(eventJson, column) match {
      case Some(value) => value.toLowerCase.contains(filterValue.toLowerCase)
      case None => false
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
