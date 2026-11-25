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
import com.snowplowanalytics.snowplow.micro.storage.PostgresqlStorage
import io.circe.Json
import doobie.implicits._
import java.time.{Duration, Instant}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class PostgresqlStorageSpec extends Specification with EventStorageTimelineSpec with EventStorageColumnStatsSpec with EventStorageFilteredEventsSpec {
  import InMemoryStorageSpec._
  import PostgresqlStorageSpec._

  sequential

  "addToGood" >> {
    "should store good events and ignore bad events" >> {
      withPostgresqlStorage { storage =>
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
      withPostgresqlStorage { storage =>
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
      withPostgresqlStorage { storage =>
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
      withPostgresqlStorage { storage =>
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
      withPostgresqlStorage { storage =>
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
      withPostgresqlStorage { storage =>
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
      withPostgresqlStorage { storage =>
        storage.getFilteredEvents(allEventsRequest).map(_.events.size must_== 0)
      }
    }
  }

  timelineTests(PostgresqlStorageSpec.postgresResource, "PostgresqlStorage")
  columnStatsTests(PostgresqlStorageSpec.postgresResource, "PostgresqlStorage")
  filteredEventsTests(PostgresqlStorageSpec.postgresResource, "PostgresqlStorage")

  "PostgresqlStorage specific column stats behavior" >> {
    "should mark fields of complex columns as non-sortable but filterable, with the search limit" >> {
      PostgresqlStorageSpec.postgresResource.use { storage =>
        val columns = List("contexts_com_example_schema_1.field1", "unstruct_event_com_example_schema_1.field1")
        storage.getColumnStats(columns).map { response =>
          columns.map(response(_)) must contain { (stats: ColumnStats) =>
            stats.sortable must beFalse
            stats.filterable must beTrue
            stats.values mustEqual Some(Nil)
            stats.searchLimit must beSome(100000)
          }.foreach
        }
      }.unsafeRunSync()
    }

    "should mark whole complex columns as non-sortable, non-filterable" >> {
      PostgresqlStorageSpec.postgresResource.use { storage =>
        storage.getColumnStats(List("contexts_com_example_schema_1", "unstruct_event_com_example_schema_1")).map { response =>
          response.values.toList must contain { (stats: ColumnStats) =>
            stats.sortable must beFalse
            stats.filterable must beFalse
            stats.values must beNone
            stats.searchLimit must beNone
          }.foreach
        }
      }.unsafeRunSync()
    }

    "should mark collector_tstamp as sortable but non-filterable, derived_tstamp as non-sortable non-filterable" >> {
      PostgresqlStorageSpec.postgresResource.use { storage =>
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
      PostgresqlStorageSpec.postgresResource.use { storage =>
        storage.getColumnStats(List("event_id", "app_id")).map { response =>
          response must haveKey("event_id")
          response must haveKey("app_id")
          response("event_id").sortable must beTrue
          response("app_id").sortable must beTrue
          response("event_id").filterable must beTrue
          response("app_id").filterable must beTrue
          response("event_id").values mustEqual Some(Nil)
          response("app_id").values mustEqual Some(Nil)
          response("event_id").searchLimit must beNone
          response("app_id").searchLimit must beNone
        }
      }.unsafeRunSync()
    }

    "should mark non-indexed standard columns as non-sortable but filterable, with the search limit" >> {
      PostgresqlStorageSpec.postgresResource.use { storage =>
        storage.getColumnStats(List("non_indexed_column", "user_ipaddress")).map { response =>
          response.values.toList must contain { (stats: ColumnStats) =>
            stats.sortable must beFalse
            stats.filterable must beTrue
            stats.values mustEqual Some(Nil)
            stats.searchLimit must beSome(100000)
          }.foreach
        }
      }.unsafeRunSync()
    }

    "should keep the same sortable columns: collector_tstamp and the indexed columns" >> {
      PostgresqlStorageSpec.postgresResource.use { storage =>
        val sortable = List("collector_tstamp", "event_id", "app_id", "event_name", "platform", "name_tracker",
          "domain_userid", "v_tracker", "user_id", "network_userid")
        val notSortable = List("derived_tstamp", "user_ipaddress", s"$UserEntityColumn.name", s"$ClickEventColumn.label", UserEntityColumn)
        storage.getColumnStats(sortable ++ notSortable).map { response =>
          sortable.map(response(_).sortable) must contain(beTrue).foreach
          notSortable.map(response(_).sortable) must contain(beFalse).foreach
        }
      }.unsafeRunSync()
    }
  }

  "PostgresqlStorage search limit for unindexed filters" >> {
    // 5 events, collected 1 to 5 seconds after GoodEvent1, all with the same values
    val events = (1 to 5).map(i => richEvent(i, "10.0.0.1", List("Zoë"), "café")).toList
    def collectedSeconds(response: EventsResponse): List[Long] =
      response.events.flatMap(_.hcursor.get[Instant]("collector_tstamp").toOption).map(_.getEpochSecond - 1761686400L).sorted
    def request(filters: EventsFilter*): EventsRequest = EventsRequest(filters.toList, None, None, None, 1, 10)

    "should only find matches among the last N events for unindexed columns" >> {
      withLimitedStorage(searchLimit = 2) { storage =>
        for {
          _ <- storage.addToGood(events)
          standard <- storage.getFilteredEvents(request(EventsFilter("user_ipaddress", List("10.0.0.1"))))
          entity <- storage.getFilteredEvents(request(EventsFilter(s"$UserEntityColumn.name", List("Zoë"))))
          unstruct <- storage.getFilteredEvents(request(EventsFilter(s"$ClickEventColumn.label", List("café"))))
          mixed <- storage.getFilteredEvents(request(EventsFilter("app_id", List("rich")), EventsFilter("user_ipaddress", List("10.0.0.1"))))
        } yield {
          List(standard, entity, unstruct, mixed).map(_.totalItems) must_== List(2, 2, 2, 2)
          List(standard, entity, unstruct, mixed).map(collectedSeconds) must contain(be_==(List(4L, 5L))).foreach
        }
      }
    }

    "should apply the limit after the time range and valid/failed conditions" >> {
      withLimitedStorage(searchLimit = 2) { storage =>
        val failed = richEvent(6, "10.0.0.1", List("Zoë"), "café").copy(incomplete = true)
        val timeRange = TimeRange(None, Some(Instant.ofEpochSecond(1761686400L + 4)))
        for {
          _ <- storage.addToGood(failed :: events)
          result <- storage.getFilteredEvents(
            EventsRequest(List(EventsFilter("user_ipaddress", List("10.0.0.1"))), Some(true), Some(timeRange), None, 1, 10)
          )
        } yield collectedSeconds(result) must_== List(2L, 3L)
      }
    }

    "should still search all events for indexed columns" >> {
      withLimitedStorage(searchLimit = 2) { storage =>
        for {
          _ <- storage.addToGood(events)
          result <- storage.getFilteredEvents(request(EventsFilter("app_id", List("rich"))))
          stats <- storage.getColumnStats(List("app_id", "user_ipaddress"))
        } yield {
          result.totalItems must_== 5
          stats("app_id").searchLimit must beNone
          stats("user_ipaddress").searchLimit must beSome(2)
        }
      }
    }

    "should sort and paginate within the last N events" >> {
      withLimitedStorage(searchLimit = 3) { storage =>
        val sorting = Some(EventsSorting("collector_tstamp", desc = false))
        val filters = List(EventsFilter("user_ipaddress", List("10.0.0.1")))
        for {
          _ <- storage.addToGood(events)
          page1 <- storage.getFilteredEvents(EventsRequest(filters, None, None, sorting, 1, 2))
          page2 <- storage.getFilteredEvents(EventsRequest(filters, None, None, sorting, 2, 2))
        } yield {
          page1.totalItems must_== 3
          page1.totalPages must_== 2
          collectedSeconds(page1) must_== List(3L, 4L)
          collectedSeconds(page2) must_== List(5L)
        }
      }
    }

    "should return each page in the requested order" >> {
      withLimitedStorage(searchLimit = 4) { storage =>
        def inPageOrder(response: EventsResponse): List[Long] =
          response.events.flatMap(_.hcursor.get[Instant]("collector_tstamp").toOption).map(_.getEpochSecond - 1761686400L)
        val filters = List(EventsFilter("user_ipaddress", List("10.0.0.1")))
        for {
          _ <- storage.addToGood(events)
          newestFirst <- storage.getFilteredEvents(EventsRequest(filters, None, None, None, 1, 3))
          oldestFirst <- storage.getFilteredEvents(
            EventsRequest(filters, None, None, Some(EventsSorting("collector_tstamp", desc = false)), 2, 3)
          )
        } yield {
          inPageOrder(newestFirst) must_== List(5L, 4L, 3L)
          inPageOrder(oldestFirst) must_== List(5L)
        }
      }
    }
  }

  "PostgresqlStorage database settings" >> {
    "should store event_json with lz4 compression in new tables" >> {
      withPostgresqlStorage { storage =>
        sql"DROP TABLE events".update.run.transact(storage.xa)
      }
      withPostgresqlStorage { storage =>
        sql"SELECT attcompression::text FROM pg_attribute WHERE attrelid = 'events'::regclass AND attname = 'event_json'"
          .query[String].unique.transact(storage.xa)
          .map(_ must_== "l")
      }
    }

    "should set a statement timeout below the HTTP server's timeout" >> {
      withPostgresqlStorage { storage =>
        sql"SHOW statement_timeout".query[String].unique.transact(storage.xa).map(_ must_== "27s")
      }
    }

    "should derive the statement timeout from the HTTP server's timeout" >> {
      (PostgresqlStorage.statementTimeout(30.seconds) must_== 27.seconds) and
        (PostgresqlStorage.statementTimeout(60.seconds) must_== 54.seconds) and
        (PostgresqlStorage.statementTimeout(1.second) must_== 900.millis)
    }
  }
}

object PostgresqlStorageSpec {
  // The HTTP server's default timeout (`networking.responseHeaderTimeout` from the collector's reference.conf)
  val httpTimeout: FiniteDuration = 30.seconds

  val allEventsRequest = EventsRequest(List.empty, None, None, None, 1, 100)

  def postgresResource: Resource[IO, PostgresqlStorage] = {
    PostgresqlStorage.create(
      host = "localhost",
      port = 5432,
      database = "micro_test",
      user = "test_user",
      password = "test_password",
      httpTimeout = httpTimeout
    ).evalTap(_.reset())
  }

  def withLimitedStorage[A](searchLimit: Int)(test: PostgresqlStorage => IO[A]): A =
    PostgresqlStorage.create(
      host = "localhost",
      port = 5432,
      database = "micro_test",
      user = "test_user",
      password = "test_password",
      httpTimeout = httpTimeout,
      unindexedSearchLimit = searchLimit
    ).evalTap(_.reset()).use(test).unsafeRunSync()

  def withPostgresqlStorage[A](test: PostgresqlStorage => IO[A]): A = {
    postgresResource
      .use(test)
      .unsafeRunSync()
  }
}