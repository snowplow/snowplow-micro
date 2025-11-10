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

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import org.specs2.mutable.Specification
import io.circe.Json

class SqliteStorageSpec extends Specification with EventStorageTimelineSpec with EventStorageColumnStatsSpec with EventStorageFilteredEventsSpec {
  import InMemoryStorageSpec._
  import SqliteStorageSpec._

  "addToGood" >> {
    "should store good events and ignore bad events" >> {
      withSqliteStorage() { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
          _ <- storage.addToBad(List(BadEvent1))
          events <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          events.events.size must_== 2
          // Events should be ordered by timestamp DESC
          events.events.map(_.hcursor.get[String]("event_id").toOption) must_==
            List(Some(GoodEvent2.event.event_id.toString), Some(GoodEvent1.event.event_id.toString))
        }
      }
    }
  }

  "reset" >> {
    "should clear all events" >> {
      withSqliteStorage() { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
          eventsBefore <- storage.getFilteredEvents(allEventsRequest)
          _ <- storage.reset()
          eventsAfter <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          eventsBefore.events.size must_== 2
          eventsAfter.events.size must_== 0
        }
      }
    }
  }

  "maxEvents" >> {
    "should limit the number of stored events when adding one by one" >> {
      // a low limit like 1 will guarantee immediate eviction
      // (much higher limits are probabilistic so harder to test)
      withSqliteStorage(Some(1)) { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1))
          events1 <- storage.getFilteredEvents(allEventsRequest)
          _ <- storage.addToGood(List(GoodEvent2))
          events2 <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          events1.events.size must_== 1
          events2.events.size must_== 1
          // Should keep the most recent event
          events2.events.head.hcursor.get[String]("event_id").toOption must
            beSome(GoodEvent2.event.event_id.toString)
        }
      }
    }

    "should limit the number of stored events across multiple batches" >> {
      // a low limit like 2 will guarantee immediate eviction
      // (much higher limits are probabilistic so harder to test)
      withSqliteStorage(Some(2)) { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
          eventsAfterFirst <- storage.getFilteredEvents(allEventsRequest)
          _ <- storage.addToGood(List(GoodEvent3))
          eventsAfterSecond <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          eventsAfterFirst.events.size must_== 2
          eventsAfterSecond.events.size must_== 2
          // Should keep one event from first batch and one from second batch
          val eventIds = eventsAfterSecond.events.map(_.hcursor.get[String]("event_id").toOption.get)
          eventIds must contain(allOf(
            GoodEvent2.event.event_id.toString, // most recent from first batch
            GoodEvent3.event.event_id.toString  // from second batch
          ))
        }
      }
    }
  }

  "getEvents" >> {
    "should return events as JSON" >> {
      withSqliteStorage() { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1))
          events <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          events.events.size must_== 1
          events.events.head must beAnInstanceOf[Json]
          events.events.head.hcursor.get[String]("event_id").toOption must
            beSome(GoodEvent1.event.event_id.toString)
        }
      }
    }

    "should return empty list when no events" >> {
      withSqliteStorage() { storage =>
        storage.getFilteredEvents(allEventsRequest).map(_.events.size must_== 0)
      }
    }
  }

  timelineTests(SqliteStorage.inMemory(None), "SqliteStorage")
  columnStatsTests(SqliteStorage.inMemory(None), "SqliteStorage")
  filteredEventsTests(SqliteStorage.inMemory(None), "SqliteStorage")
}

object SqliteStorageSpec {
  val allEventsRequest = EventsRequest(List.empty, None, None, None, 1, 100)

  def withSqliteStorage[A](maxEvents: Option[Int] = None)(test: SqliteStorage => IO[A]): A = {
    SqliteStorage.inMemory(maxEvents)
      .use(test)
      .unsafeRunSync()
  }
}