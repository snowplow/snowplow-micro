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
import cats.effect.{IO, Resource}
import org.specs2.mutable.Specification
import io.circe.Json
import java.time.{Duration, Instant}
import scala.concurrent.duration.DurationInt

class SqliteStorageSpec extends Specification with EventStorageTimelineSpec with EventStorageColumnStatsSpec with EventStorageFilteredEventsSpec {
  import InMemoryStorageSpec._
  import SqliteStorageSpec._

  "addToGood" >> {
    "should store good events and ignore bad events" >> {
      withSqliteStorage { storage =>
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
      withSqliteStorage { storage =>
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

  "cleanupExpiredEvents" >> {
    "should delete events older than TTL" >> {
      withSqliteStorage { storage =>
        // Create events with different timestamps
        val now = Instant.now()
        val oldEvent = GoodEvent1.copy(event = GoodEvent1.event.copy(collector_tstamp = now.minus(Duration.ofHours(2))))
        val recentEvent = GoodEvent2.copy(event = GoodEvent2.event.copy(collector_tstamp = now.minus(Duration.ofMinutes(30))))

        for {
          _ <- storage.addToGood(List(oldEvent, recentEvent))
          eventsBefore <- storage.getFilteredEvents(allEventsRequest)
          _ <- storage.cleanupExpiredEvents(1.hour)
          eventsAfter <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          eventsBefore.events.size must_== 2
          eventsAfter.events.size must_== 1
          // Should keep the recent event
          eventsAfter.events.head.hcursor.get[String]("event_id").toOption must
            beSome(GoodEvent2.event.event_id.toString)
        }
      }
    }

    "should keep all events when none exceed TTL" >> {
      withSqliteStorage { storage =>
        val now = Instant.now()
        val recentEvent1 = GoodEvent1.copy(event = GoodEvent1.event.copy(collector_tstamp = now.minus(Duration.ofMinutes(10))))
        val recentEvent2 = GoodEvent2.copy(event = GoodEvent2.event.copy(collector_tstamp = now.minus(Duration.ofMinutes(5))))

        for {
          _ <- storage.addToGood(List(recentEvent1, recentEvent2))
          eventsBefore <- storage.getFilteredEvents(allEventsRequest)
          _ <- storage.cleanupExpiredEvents(1.hour)
          eventsAfter <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          eventsBefore.events.size must_== 2
          eventsAfter.events.size must_== 2
        }
      }
    }

    "should delete all events when all exceed TTL" >> {
      withSqliteStorage { storage =>
        val now = Instant.now()
        val oldEvent1 = GoodEvent1.copy(event = GoodEvent1.event.copy(collector_tstamp = now.minus(Duration.ofHours(3))))
        val oldEvent2 = GoodEvent2.copy(event = GoodEvent2.event.copy(collector_tstamp = now.minus(Duration.ofHours(2))))

        for {
          _ <- storage.addToGood(List(oldEvent1, oldEvent2))
          eventsBefore <- storage.getFilteredEvents(allEventsRequest)
          _ <- storage.cleanupExpiredEvents(1.hour)
          eventsAfter <- storage.getFilteredEvents(allEventsRequest)
        } yield {
          eventsBefore.events.size must_== 2
          eventsAfter.events.size must_== 0
        }
      }
    }
  }

  "getEvents" >> {
    "should return events as JSON" >> {
      withSqliteStorage { storage =>
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
      withSqliteStorage { storage =>
        storage.getFilteredEvents(allEventsRequest).map(_.events.size must_== 0)
      }
    }
  }

  timelineTests(SqliteStorageSpec.freshDbResource, "SqliteStorage")
  columnStatsTests(SqliteStorageSpec.freshDbResource, "SqliteStorage")
  filteredEventsTests(SqliteStorageSpec.freshDbResource, "SqliteStorage")

  "SqliteStorage specific column stats behavior" >> {
    "should mark non-indexed complex columns as non-sortable, non-filterable" >> {
      SqliteStorageSpec.freshDbResource.use { storage =>
        storage.getColumnStats(List("contexts_com_example_schema_1.field1")).map { response =>
          response must haveKey("contexts_com_example_schema_1.field1")
          response("contexts_com_example_schema_1.field1").sortable must beFalse
          response("contexts_com_example_schema_1.field1").filterable must beFalse
          response("contexts_com_example_schema_1.field1").values must beNone
        }
      }.unsafeRunSync()
    }

    "should mark collector_tstamp as sortable but non-filterable, derived_tstamp as non-sortable non-filterable" >> {
      SqliteStorageSpec.freshDbResource.use { storage =>
        storage.getColumnStats(List("collector_tstamp", "derived_tstamp")).map { response =>
          response must haveKey("collector_tstamp")
          response must haveKey("derived_tstamp")
          response("collector_tstamp").sortable must beTrue
          response("derived_tstamp").sortable must beFalse
          response("collector_tstamp").filterable must beFalse
          response("derived_tstamp").filterable must beFalse
          response("collector_tstamp").values must beNone
          response("derived_tstamp").values must beNone
        }
      }.unsafeRunSync()
    }

    "should mark indexed columns as sortable and filterable" >> {
      SqliteStorageSpec.freshDbResource.use { storage =>
        storage.getColumnStats(List("event_id", "app_id")).map { response =>
          response must haveKey("event_id")
          response must haveKey("app_id")
          response("event_id").sortable must beTrue
          response("app_id").sortable must beTrue
          response("event_id").filterable must beTrue
          response("app_id").filterable must beTrue
          response("event_id").values mustEqual Some(Nil)
          response("app_id").values mustEqual Some(Nil)
        }
      }.unsafeRunSync()
    }

    "should mark non-indexed simple columns as non-sortable, non-filterable" >> {
      SqliteStorageSpec.freshDbResource.use { storage =>
        storage.getColumnStats(List("non_indexed_column")).map { response =>
          response must haveKey("non_indexed_column")
          response("non_indexed_column").sortable must beFalse
          response("non_indexed_column").filterable must beFalse
          response("non_indexed_column").values must beNone
        }
      }.unsafeRunSync()
    }

    "should mark non-existent columns as non-sortable and non-filterable in SQLite mode" >> {
      SqliteStorageSpec.freshDbResource.use { storage =>
        storage.getColumnStats(List("non_existent_column")).map { response =>
          response must haveKey("non_existent_column")
          response("non_existent_column").sortable must beFalse
          response("non_existent_column").filterable must beFalse
          response("non_existent_column").values must beNone
        }
      }.unsafeRunSync()
    }
  }
}

object SqliteStorageSpec {
  val allEventsRequest = EventsRequest(List.empty, None, None, None, 1, 100)

  private def createTempDbPath(): String = {
    val tmpDir = System.getProperty("java.io.tmpdir")
    s"$tmpDir/snowplow-micro-test-${System.nanoTime()}.db"
  }

  private def createTempDbResource(): Resource[IO, SqliteStorage] = {
    Resource.make(IO(createTempDbPath()))(path => IO {
      // Clean up database file and WAL files after test
      new java.io.File(path).delete()
      new java.io.File(s"$path-shm").delete()
      new java.io.File(s"$path-wal").delete()
      ()
    }).flatMap(SqliteStorage.file)
  }

  def freshDbResource: Resource[IO, EventStorage] = {
    for {
      path <- Resource.eval(IO(createTempDbPath()))
      storage <- Resource.make(IO(path))(path => IO {
        // Clean up database file and WAL files after test
        new java.io.File(path).delete()
        new java.io.File(s"$path-shm").delete()
        new java.io.File(s"$path-wal").delete()
        ()
      }).flatMap(SqliteStorage.file)
    } yield storage
  }

  def withSqliteStorage[A](test: SqliteStorage => IO[A]): A = {
    createTempDbResource()
      .use(test)
      .unsafeRunSync()
  }
}
