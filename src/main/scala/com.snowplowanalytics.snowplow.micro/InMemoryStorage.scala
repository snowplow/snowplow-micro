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
  def addToGood(events: List[GoodEvent]): IO[Unit] = IO.delay {
    LockGood.synchronized {
      good = events ++ good
    }
  }

  /** Add a bad event to the cache. */
  def addToBad(events: List[BadEvent]): IO[Unit] = IO.delay {
    LockBad.synchronized {
      bad = events ++ bad
    }
  }

  /** Remove all the events from memory. */
  def reset(): IO[Unit] = IO.delay {
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

  /** Get all good + incomplete events */
  def getGoodAndIncomplete: List[GoodEvent] =
    LockGood.synchronized(good)

  /** Filter out the bad events with the possible filters contained in the HTTP request. */
  def filterBad(
    filtersBad: FiltersBad = FiltersBad(None, None, None)
  ): List[BadEvent] =
    LockBad.synchronized {
      val filtered = bad.filter(keepBadEvent(_, filtersBad))
      filtered.take(filtersBad.limit.getOrElse(filtered.size))
    }
}

private[micro] object InMemoryStorage {
  /** Check if a good event matches the filters. */
  private[micro] def keepGoodEvent(event: GoodEvent, filters: FiltersGood): Boolean =
    filters.event_type.toSet.subsetOf(event.eventType.toSet) &&
      filters.schema.toSet.subsetOf(event.schema.toSet) &&
      filters.contexts.forall(containsAllContexts(event, _))

  /** Check if an event conntains all the contexts of the list. */
  private[micro] def containsAllContexts(event: GoodEvent, contexts: List[String]): Boolean =
    contexts.forall(event.contexts.contains)

  /** Check if a bad event matches the filters. */
  private[micro] def keepBadEvent(event: BadEvent, filters: FiltersBad): Boolean =
    filters.vendor.forall(vendor => event.collectorPayload.forall(_ .api.vendor == vendor)) &&
      filters.version.forall(version => event.collectorPayload.forall(_ .api.version == version))
}