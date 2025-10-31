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
          events <- storage.getEvents
        } yield {
          events.size must_== 2
          // Events should be ordered by timestamp DESC
          events.map(_.hcursor.get[String]("event_id").toOption) must_==
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
          eventsBefore <- storage.getEvents
          _ <- storage.reset()
          eventsAfter <- storage.getEvents
        } yield {
          eventsBefore.size must_== 2
          eventsAfter.size must_== 0
        }
      }
    }
  }

  "maxEvents" >> {
    "should limit the number of stored events when adding one by one" >> {
      withSqliteStorage(Some(1)) { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1))
          events1 <- storage.getEvents
          _ <- storage.addToGood(List(GoodEvent2))
          events2 <- storage.getEvents
        } yield {
          events1.size must_== 1
          events2.size must_== 1
          // Should keep the most recent event
          events2.head.hcursor.get[String]("event_id").toOption must
            beSome(GoodEvent2.event.event_id.toString)
        }
      }
    }

    "should limit the number of stored events across multiple batches" >> {
      withSqliteStorage(Some(2)) { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
          eventsAfterFirst <- storage.getEvents
          _ <- storage.addToGood(List(GoodEvent3))
          eventsAfterSecond <- storage.getEvents
        } yield {
          eventsAfterFirst.size must_== 2
          eventsAfterSecond.size must_== 2
          // Should keep one event from first batch and one from second batch
          val eventIds = eventsAfterSecond.map(_.hcursor.get[String]("event_id").toOption.get)
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
          events <- storage.getEvents
        } yield {
          events.size must_== 1
          events.head must beAnInstanceOf[Json]
          events.head.hcursor.get[String]("event_id").toOption must
            beSome(GoodEvent1.event.event_id.toString)
        }
      }
    }

    "should return empty list when no events" >> {
      withSqliteStorage() { storage =>
        storage.getEvents.map(_.size must_== 0)
      }
    }
  }

  timelineTests(SqliteStorage.inMemory(None), "SqliteStorage")
  columnStatsTests(SqliteStorage.inMemory(None), "SqliteStorage")
  filteredEventsTests(SqliteStorage.inMemory(None), "SqliteStorage")

  "getTimeline with maxEvents limit" >> {
    "should handle timeline with maxEvents limit" >> {
      withSqliteStorage(Some(2)) { storage =>
        for {
          _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
          timeline <- storage.getTimeline
        } yield {
          timeline.points must have size(31)
          val pointsWithEvents = timeline.points.filter(p => p.validEvents > 0 || p.failedEvents > 0)
          // Should still show timeline based on all events in database (limited by cleanup probability)
          pointsWithEvents must have size(be_>=(1))
        }
      }
    }
  }
}

object SqliteStorageSpec {
  def withSqliteStorage[A](maxEvents: Option[Int] = None)(test: SqliteStorage => IO[A]): A = {
    SqliteStorage.inMemory(maxEvents)
      .use(test)
      .unsafeRunSync()
  }
}