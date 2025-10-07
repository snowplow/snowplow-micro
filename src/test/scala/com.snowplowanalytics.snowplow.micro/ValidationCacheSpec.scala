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

import org.specs2.mutable.Specification
import org.joda.time.DateTime

import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import java.util.UUID
import java.time.Instant

class ValidationCacheSpec extends Specification {
  import ValidationCacheSpec._

  "getSummary" >> {
    "should return the correct number of bad and good events" >> {
      "for an empty cache" >> {
        val cache = emptyCache()

        cache.getSummary().good must_== 0
        cache.getSummary().bad must_== 0
        cache.getSummary().total must_== 0
      }

      "for a non-empty cache" >> {
        val cache = cacheOf(
          List(GoodEvent1, GoodEvent2),
          List(BadEvent1)
        )

        cache.getSummary().good must_== 2
        cache.getSummary().bad must_== 1
        cache.getSummary().total must_== 3

        cache.filterGood() must contain(exactly(GoodEvent1, GoodEvent2))
        cache.filterBad() must contain(exactly(BadEvent1))
      }
    }
  }

  "addToGood" >> {
    "should succesfully add one good event to an empty cache" >> {
      val cache = emptyCache()
      cache.addToGood(List(GoodEvent1))

      cache.getSummary().good must_== 1
      cache.getSummary().bad must_== 0
      cache.filterGood() must contain(exactly(GoodEvent1))
    }

    "should succesfully add several good events to an empty cache" >> {
      val cache = emptyCache()
      cache.addToGood(List(GoodEvent1, GoodEvent2))

      cache.getSummary().good must_== 2
      cache.getSummary().bad must_== 0
      cache.filterGood() must contain(exactly(GoodEvent1, GoodEvent2))
    }

    "should succesfully add good events to a non empty cache" >> {
      val cache = cacheOf(List(GoodEvent1), Nil)
      cache.addToGood(List(GoodEvent2))

      cache.getSummary().good must_== 2
      cache.getSummary().bad must_== 0
      cache.filterGood() must contain(exactly(GoodEvent1, GoodEvent2))
    }

    "should respect the limit when adding events" >> {
      val cache = new ValidationCache(Some(1))
      cache.addToGood(List(GoodEvent1, GoodEvent2))

      cache.getSummary().good must_== 1
      cache.filterGood() must contain(exactly(GoodEvent1))
    }

    "should implement rolling behavior when limit is exceeded" >> {
      val cache = new ValidationCache(Some(1))
      cache.addToGood(List(GoodEvent1))
      cache.addToGood(List(GoodEvent2))

      cache.getSummary().good must_== 1
      cache.filterGood() must contain(exactly(GoodEvent2))
    }

    "should maintain FIFO order with rolling cache" >> {
      val cache = new ValidationCache(Some(2))
      cache.addToGood(List(GoodEvent1))
      cache.addToGood(List(GoodEvent2))

      cache.getSummary().good must_== 2
      cache.filterGood() must contain(exactly(GoodEvent1, GoodEvent2))

      // Add third event, should drop the first (GoodEvent1)
      val GoodEvent3 = GoodEvent1.copy(eventType = Some("type3"))
      cache.addToGood(List(GoodEvent3))

      cache.getSummary().good must_== 2
      cache.filterGood() must contain(exactly(GoodEvent2, GoodEvent3))
    }
  }

  "addToBad" >> {
    "should succesfully add one bad event to an empty cache" >> {
      val cache = emptyCache()
      cache.addToBad(List(BadEvent1))

      cache.getSummary().good must_== 0
      cache.getSummary().bad must_== 1
      cache.filterBad() must contain(exactly(BadEvent1))
    }

    "should succesfully add several bad events to an empty cache" >> {
      val cache = emptyCache()
      cache.addToBad(List(BadEvent1, BadEvent2))

      cache.getSummary().good must_== 0
      cache.getSummary().bad must_== 2
      cache.filterBad() must contain(exactly(BadEvent1, BadEvent2))
    }

    "should succesfully add bad events to a non empty cache" >> {
      val cache = cacheOf(Nil, List(BadEvent1))
      cache.addToBad(List(BadEvent2))

      cache.getSummary().good must_== 0
      cache.getSummary().bad must_== 2
      cache.filterBad() must contain(exactly(BadEvent1, BadEvent2))
    }

    "should respect the limit when adding bad events" >> {
      val cache = new ValidationCache(Some(1))
      cache.addToBad(List(BadEvent1, BadEvent2))

      cache.getSummary().bad must_== 1
      cache.filterBad() must contain(exactly(BadEvent1))
    }

    "should implement rolling behavior for bad events when limit is exceeded" >> {
      val cache = new ValidationCache(Some(1))
      cache.addToBad(List(BadEvent1))
      cache.addToBad(List(BadEvent2))

      cache.getSummary().bad must_== 1
      cache.filterBad() must contain(exactly(BadEvent2))
    }
  }

  "reset" >> {
    "should remove all the good and bad events from the cache" >> {
      val cache = cacheOf(List(GoodEvent1), List(BadEvent1))
      cache.reset()
      cache.getSummary().good must_== 0
      cache.getSummary().bad must_== 0
    }
  }

  "filterGood" >> {
    "should return only the good events that match the filtered event type" >> {
      val cache = cacheOf(List(GoodEvent1, GoodEvent2), Nil)

      val filter1 = EmptyFilterGood.copy(event_type = GoodEvent1.eventType)
      val filter2 = EmptyFilterGood.copy(event_type = GoodEvent2.eventType)
      val filterNonsense = EmptyFilterGood.copy(event_type = Some("nonsense"))

      cache.filterGood(filter1) must contain(exactly(GoodEvent1))
      cache.filterGood(filter2) must contain(exactly(GoodEvent2))
      cache.filterGood(filterNonsense) must be empty
    }

    "should return only the good events that match the filtered schema" >> {
      val cache = cacheOf(List(GoodEvent1, GoodEvent2), Nil)

      val filter1 = EmptyFilterGood.copy(schema = GoodEvent1.schema)
      val filter2 = EmptyFilterGood.copy(schema = GoodEvent2.schema)
      val filterNonsense = EmptyFilterGood.copy(schema = Some("nonsense"))

      cache.filterGood(filter1) must contain(exactly(GoodEvent1))
      cache.filterGood(filter2) must contain(exactly(GoodEvent2))
      cache.filterGood(filterNonsense) must be empty
    }

    "should return only the good events that match the filtered context" >> {
      val cache = cacheOf(List(GoodEvent1, GoodEvent2), Nil)

      val filter1a = EmptyFilterGood.copy(contexts = Some(GoodEvent1.contexts.headOption.toList))
      val filter1b = EmptyFilterGood.copy(contexts = Some(GoodEvent1.contexts.tail))
      val filter1c = EmptyFilterGood.copy(contexts = Some(GoodEvent1.contexts))

      val filter2a = EmptyFilterGood.copy(contexts = Some(GoodEvent2.contexts.headOption.toList))
      val filter2b = EmptyFilterGood.copy(contexts = Some(GoodEvent2.contexts.tail))
      val filter2c = EmptyFilterGood.copy(contexts = Some(GoodEvent2.contexts))


      val filterAllContexts = EmptyFilterGood.copy(contexts = Some(GoodEvent1.contexts ::: GoodEvent2.contexts))
      val filterNonsense = EmptyFilterGood.copy(contexts = Some(List("nonsense")))


      cache.filterGood(filter1a) must contain(exactly(GoodEvent1))
      cache.filterGood(filter1b) must contain(exactly(GoodEvent1))
      cache.filterGood(filter1c) must contain(exactly(GoodEvent1))

      cache.filterGood(filter2a) must contain(exactly(GoodEvent2))
      cache.filterGood(filter2b) must contain(exactly(GoodEvent2))
      cache.filterGood(filter2c) must contain(exactly(GoodEvent2))

      cache.filterGood(filterAllContexts) must have size(0)
      cache.filterGood(filterNonsense) must have size(0)
    }

    "should return the most recent events if limit is set" >> {
      val cache = emptyCache()
      cache.addToGood(List(GoodEvent1))
      cache.addToGood(List(GoodEvent2)) // most recent

      val filter = EmptyFilterGood.copy(limit = Some(1))
      cache.filterGood(filter) must contain(exactly(GoodEvent2))
    }
  }

  "keepGoodEvent" >> {
    "should correctly return true or false if the filter contains only an event_type and it matches" >> {
      val shouldKeep = EmptyFilterGood.copy(event_type = GoodEvent1.eventType)
      val shouldNotKeep = EmptyFilterGood.copy(event_type = GoodEvent2.eventType)

      ValidationCache.keepGoodEvent(GoodEvent1, shouldKeep) should beTrue
      ValidationCache.keepGoodEvent(GoodEvent1, shouldNotKeep) should beFalse
    }

    "should correctly return true or false if the filter contains only a schema and it matches" >> {
      val shouldKeep = EmptyFilterGood.copy(schema = GoodEvent1.schema)
      val shouldNotKeep = EmptyFilterGood.copy(schema = GoodEvent2.schema)

      ValidationCache.keepGoodEvent(GoodEvent1, shouldKeep) should beTrue
      ValidationCache.keepGoodEvent(GoodEvent1, shouldNotKeep) should beFalse
    }

    "should correctly return true or false if the filter contains only contexts and it matches" >> {
      val shouldKeep = EmptyFilterGood.copy(contexts = Some(GoodEvent1.contexts))
      val shouldNotKeep = EmptyFilterGood.copy(contexts = Some(GoodEvent2.contexts))

      ValidationCache.keepGoodEvent(GoodEvent1, shouldKeep) should beTrue
      ValidationCache.keepGoodEvent(GoodEvent1, shouldNotKeep) should beFalse
    }

    "should correctly return true or false if several filters are set at the same time" >> {
      val shouldKeep = FiltersGood(GoodEvent1.eventType, GoodEvent1.schema, Some(GoodEvent1.contexts), None)
      val shouldNotKeep1 = FiltersGood(Some("nonsense"), GoodEvent1.schema, Some(GoodEvent1.contexts), None)
      val shouldNotKeep2 = FiltersGood(GoodEvent1.eventType, Some("nonsense"), Some(GoodEvent1.contexts), None)
      val shouldNotKeep3 = FiltersGood(GoodEvent1.eventType, GoodEvent1.schema, Some(List("nonsense")), None)

      ValidationCache.keepGoodEvent(GoodEvent1, shouldKeep) should beTrue
      ValidationCache.keepGoodEvent(GoodEvent1, shouldNotKeep1) should beFalse
      ValidationCache.keepGoodEvent(GoodEvent1, shouldNotKeep2) should beFalse
      ValidationCache.keepGoodEvent(GoodEvent1, shouldNotKeep3) should beFalse
    }
  }

  "containsAllContext" >> {
    "should return true if the event contains exactly the same contexts" >> {
      ValidationCache.containsAllContexts(GoodEvent1, GoodEvent1.contexts) should beTrue
    }

    "should return true if the event contains the same contexts and more" >> {
      ValidationCache.containsAllContexts(GoodEvent1, GoodEvent1.contexts.tail) should beTrue
    }

    "should return true if list of contexts in the filter is empty" >> {
      ValidationCache.containsAllContexts(GoodEvent1, Nil) should beTrue
    }
  }

  "filterBad" >> {
    "should return only the bad events that match the filtered vendor" >> {
      val cache = cacheOf(Nil, List(BadEvent1, BadEvent2))

      val filter1 = EmptyFilterBad.copy(vendor = Some(CollectorPayload1.api.vendor))
      val filter2 = EmptyFilterBad.copy(vendor = Some(CollectorPayload2.api.vendor))
      val filterNonsense = EmptyFilterBad.copy(vendor = Some("nonsense"))

      cache.filterBad(filter1) must contain(exactly(BadEvent1))
      cache.filterBad(filter2) must contain(exactly(BadEvent2))
      cache.filterBad(filterNonsense) must be empty
    }

    "should return only the bad events that match the filtered version" >> {
      val cache = cacheOf(Nil, List(BadEvent1, BadEvent2))

      val filter1 = EmptyFilterBad.copy(version = Some(CollectorPayload1.api.version))
      val filter2 = EmptyFilterBad.copy(version = Some(CollectorPayload2.api.version))
      val filterNonsense = EmptyFilterBad.copy(version = Some("nonsense"))

      cache.filterBad(filter1) must contain(exactly(BadEvent1))
      cache.filterBad(filter2) must contain(exactly(BadEvent2))
      cache.filterBad(filterNonsense) must be empty
    }

    "should return the most recent events if limit is set" >> {
      val cache = emptyCache()
      cache.addToBad(List(BadEvent1))
      cache.addToBad(List(BadEvent2)) // most recent

      val filter = EmptyFilterBad.copy(limit = Some(1))
      cache.filterBad(filter) must contain(exactly(BadEvent2))
    }
  }

  "keepBadEvent" >> {
    "should correctly filter out based on the vendor" >> {
      val shouldKeep = EmptyFilterBad.copy(vendor = Some(CollectorPayload1.api.vendor))
      val shouldNotKeep = EmptyFilterBad.copy(vendor = Some(CollectorPayload2.api.vendor))

      ValidationCache.keepBadEvent(BadEvent1, shouldKeep) should beTrue
      ValidationCache.keepBadEvent(BadEvent1, shouldNotKeep) should beFalse
    }

    "should correctly filter out based on the version" >> {
      val shouldKeep = EmptyFilterBad.copy(version = Some(CollectorPayload1.api.version))
      val shouldNotKeep = EmptyFilterBad.copy(version = Some(CollectorPayload2.api.version))

      ValidationCache.keepBadEvent(BadEvent1, shouldKeep) should beTrue
      ValidationCache.keepBadEvent(BadEvent1, shouldNotKeep) should beFalse
    }

    "should correctly filter out based on the vendor and the version at the same time" >> {
      val shouldKeep = FiltersBad(Some(CollectorPayload1.api.vendor), Some(CollectorPayload1.api.version), None)
      val shouldNotKeep1 = FiltersBad(Some("nonsense"), Some(CollectorPayload1.api.version), None)
      val shouldNotKeep2 = FiltersBad(Some(CollectorPayload1.api.vendor), Some("nonsense"), None)

      ValidationCache.keepBadEvent(BadEvent1, shouldKeep) should beTrue
      ValidationCache.keepBadEvent(BadEvent1, shouldNotKeep1) should beFalse
      ValidationCache.keepBadEvent(BadEvent1, shouldNotKeep2) should beFalse
    }
  }
}

object ValidationCacheSpec {

  val EmptyFilterGood: FiltersGood = FiltersGood(None, None, None, None)
  val EmptyFilterBad: FiltersBad = FiltersBad(None, None, None)

  val testTimestamp = DateTime.parse("2014-01-16T00:49:58.278+00:00")

  def cacheOf(goodInit: List[GoodEvent], badInit: List[BadEvent]): ValidationCache = {
    val cache = new ValidationCache(None)
    cache.addToGood(goodInit)
    cache.addToBad(badInit)
    cache
  }

  def emptyCache(): ValidationCache =
    cacheOf(Nil, Nil)

  val GoodEvent1: GoodEvent =
    GoodEvent(
      events.buildRawEvent(),
      Some("type1"),
      Some("com.snowplowanalytics.example1"),
      List("com.snowplowanalytics.context1a", "com.snowplowanalytics.context1b"),
      Event.minimal(UUID.randomUUID, Instant.now, "collector1", "etl1")
    )

  val GoodEvent2: GoodEvent =
    GoodEvent(
      events.buildRawEvent(),
      Some("type2"),
      Some("com.snowplowanalytics.example2"),
      List("com.snowplowanalytics.context2a", "com.snowplowanalytics.context2b"),
      Event.minimal(UUID.randomUUID, Instant.now, "collector1", "etl1")
    )

  val CollectorPayload1: CollectorPayload =
    CollectorPayload(
      CollectorPayload.Api("vendor1", "version1"),
      Nil,
      None,
      None,
      CollectorPayload.Source("name1", "utf-8", None),
      CollectorPayload.Context(testTimestamp, None, None, None, Nil, None)
    )

  val CollectorPayload2: CollectorPayload =
    CollectorPayload(
      CollectorPayload.Api("vendor2", "version2"),
      Nil,
      None,
      None,
      CollectorPayload.Source("name2", "utf-8", None),
      CollectorPayload.Context(testTimestamp, None, None, None, Nil, None)
    )

  val BadEvent1: BadEvent =
    BadEvent(Some(CollectorPayload1), None, List("event1 is bad"))

  val BadEvent2: BadEvent =
    BadEvent(Some(CollectorPayload2), None, List("event2 is bad"))

}
