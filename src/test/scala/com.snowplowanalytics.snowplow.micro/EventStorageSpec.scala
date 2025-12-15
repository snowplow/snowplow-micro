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

import cats.effect.{IO, Resource}
import cats.effect.unsafe.implicits.global
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragment
import io.circe.Json
import io.circe.parser.parse

import java.time.temporal.ChronoUnit

class EventStorageSpec extends Specification {
  "extractColumnsFromEvent" >> {
    "should extract top-level column names" >> {
      val json = parse("""
        {
          "event_id": "123",
          "collector_tstamp": "2023-01-01T00:00:00Z",
          "app_id": "test-app"
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(exactly("event_id", "collector_tstamp", "app_id"))
    }

    "should extract nested columns from contexts_ fields" >> {
      val json = parse("""
        {
          "event_id": "123",
          "contexts_com_example_schema_1": [
            {
              "field1": "value1",
              "field2": "value2"
            },
            {
              "field1": "value3",
              "field3": "value4"
            }
          ]
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(allOf(
        "event_id",
        "contexts_com_example_schema_1",
        "contexts_com_example_schema_1.field1",
        "contexts_com_example_schema_1.field2",
        "contexts_com_example_schema_1.field3"
      ))
    }

    "should extract nested columns from unstruct_event_ fields" >> {
      val json = parse("""
        {
          "event_id": "123",
          "unstruct_event_com_example_action_1": {
            "action": "click",
            "target": "button",
            "value": 42
          }
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(allOf(
        "event_id",
        "unstruct_event_com_example_action_1",
        "unstruct_event_com_example_action_1.action",
        "unstruct_event_com_example_action_1.target",
        "unstruct_event_com_example_action_1.value"
      ))
    }

    "should handle mixed top-level, contexts, and unstruct_event fields" >> {
      val json = parse("""
        {
          "event_id": "123",
          "collector_tstamp": "2023-01-01T00:00:00Z",
          "contexts_com_example_user_1": [
            {
              "user_id": "user123",
              "session_id": "session456"
            }
          ],
          "unstruct_event_com_example_purchase_1": {
            "product_id": "prod789",
            "amount": 99.99
          }
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(allOf(
        "event_id",
        "collector_tstamp",
        "contexts_com_example_user_1",
        "contexts_com_example_user_1.user_id",
        "contexts_com_example_user_1.session_id",
        "unstruct_event_com_example_purchase_1",
        "unstruct_event_com_example_purchase_1.product_id",
        "unstruct_event_com_example_purchase_1.amount"
      ))
    }

    "should handle empty contexts arrays" >> {
      val json = parse("""
        {
          "event_id": "123",
          "contexts_com_example_schema_1": []
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(exactly("event_id", "contexts_com_example_schema_1"))
    }

    "should handle null values in JSON" >> {
      val json = parse("""
        {
          "event_id": "123",
          "nullable_field": null,
          "contexts_com_example_schema_1": [
            {
              "field1": "value1",
              "nullable_nested": null
            }
          ]
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(allOf(
        "event_id",
        "nullable_field",
        "contexts_com_example_schema_1",
        "contexts_com_example_schema_1.field1",
        "contexts_com_example_schema_1.nullable_nested"
      ))
    }

    "should not extract nested fields from non-contexts/unstruct_event fields" >> {
      val json = parse("""
        {
          "event_id": "123",
          "some_object_field": {
            "nested": "value"
          },
          "contexts_com_example_schema_1": [
            {
              "field1": "value1"
            }
          ]
        }
      """).getOrElse(Json.Null)

      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must contain(allOf(
        "event_id",
        "some_object_field",
        "contexts_com_example_schema_1",
        "contexts_com_example_schema_1.field1"
      ))
      columns must not contain("some_object_field.nested")
    }

    "should return empty set for empty JSON object" >> {
      val json = parse("{}").getOrElse(Json.Null)
      val columns = EventStorage.extractColumnsFromEvent(json)
      columns must be empty
    }
  }
}

trait EventStorageTimelineSpec {
  self: Specification =>

  import InMemoryStorageSpec._

  def timelineTests(storageResource: Resource[IO, EventStorage], storageName: String): Fragment = {
    s"$storageName getTimeline" >> {
      "should return empty timeline for empty storage" >> {
        storageResource.use { storage =>
          storage.getTimeline.map { timeline =>
            timeline.points must have size(31)
            timeline.points.forall(p => p.validEvents == 0 && p.failedEvents == 0) must beTrue
          }
        }.unsafeRunSync()
      }

      "should return timeline with events grouped by minute" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent3, GoodEvent2, GoodEvent1))
            timeline <- storage.getTimeline
          } yield {
            timeline.points must have size(31)

            val pointsWithEvents = timeline.points.filter(p => p.validEvents > 0 || p.failedEvents > 0)
            pointsWithEvents must have size(2) // GoodEvent1 & GoodEvent2 in same minute, GoodEvent3 in different minute

            val eventMinute12 = GoodEvent1.event.collector_tstamp.truncatedTo(ChronoUnit.MINUTES)
            val eventMinute3 = GoodEvent3.event.collector_tstamp.truncatedTo(ChronoUnit.MINUTES)

            val minute12Point = pointsWithEvents.find(_.timestamp == eventMinute12)
            val minute3Point = pointsWithEvents.find(_.timestamp == eventMinute3)

            minute12Point must beSome
            minute3Point must beSome

            minute12Point.get.validEvents must_== 2
            minute12Point.get.failedEvents must_== 0

            minute3Point.get.validEvents must_== 1
            minute3Point.get.failedEvents must_== 0
          }
        }.unsafeRunSync()
      }

      "should return timeline with failed events when incomplete" >> {
        val failedEvent = GoodEvent3.copy(incomplete = true)
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(failedEvent, GoodEvent2, GoodEvent1))
            timeline <- storage.getTimeline
          } yield {
            timeline.points must have size(31)

            val pointsWithEvents = timeline.points.filter(p => p.validEvents > 0 || p.failedEvents > 0)
            pointsWithEvents must have size(2)

            val eventMinute12 = GoodEvent1.event.collector_tstamp.truncatedTo(ChronoUnit.MINUTES)
            val eventMinute3 = failedEvent.event.collector_tstamp.truncatedTo(ChronoUnit.MINUTES)

            val minute12Point = pointsWithEvents.find(_.timestamp == eventMinute12)
            val minute3Point = pointsWithEvents.find(_.timestamp == eventMinute3)

            minute12Point must beSome
            minute3Point must beSome

            minute12Point.get.validEvents must_== 2
            minute12Point.get.failedEvents must_== 0

            minute3Point.get.validEvents must_== 0
            minute3Point.get.failedEvents must_== 1
          }
        }.unsafeRunSync()
      }

      "should return timeline ordered by timestamp descending" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            timeline <- storage.getTimeline
          } yield {
            timeline.points must have size(31)
            timeline.points.map(_.timestamp.toEpochMilli) must beSorted(Ordering.Long.reverse)
          }
        }.unsafeRunSync()
      }
    }
  }
}

trait EventStorageColumnStatsSpec {
  self: Specification =>

  import InMemoryStorageSpec._

  def columnStatsTests(storageResource: Resource[IO, EventStorage], storageName: String): Fragment = {
    s"$storageName getColumnStats" >> {
      "should return metadata for all columns even when storage is empty" >> {
        storageResource.use { storage =>
          storage.getColumnStats(List("event_id", "app_id")).map { response =>
            response must haveKeys("event_id", "app_id")
            response("event_id").values mustEqual Some(Nil)
            response("app_id").values mustEqual Some(Nil)
          }
        }.unsafeRunSync()
      }

      "should return distinct values for simple columns" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            response <- storage.getColumnStats(List("event_id", "app_id"))
          } yield {
            response must haveKey("event_id")
            response must haveKey("app_id")

            response("event_id").values must beSome(contain(exactly(
              GoodEvent1.event.event_id.toString,
              GoodEvent2.event.event_id.toString,
              GoodEvent3.event.event_id.toString
            )))

            response("app_id").values must beSome(contain(exactly("test1", "test2", "test3")))
          }
        }.unsafeRunSync()
      }


      "should limit to 20 distinct values" >> {
        val manyEvents = (1 to 25).map { i =>
          GoodEvent1.copy(
            event = GoodEvent1.event.copy(event_id = java.util.UUID.fromString(f"00000000-0000-0000-0000-${i}%012d"))
          )
        }.toList

        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(manyEvents)
            response <- storage.getColumnStats(List("event_id"))
          } yield {
            response must haveKey("event_id")
            response("event_id").values must beSome.like {
              case values => values must have size(20)
            }
          }
        }.unsafeRunSync()
      }

      "should return stats for supported columns" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            response <- storage.getColumnStats(List("app_id"))
          } yield {
            response must haveKey("app_id")
            response("app_id").values must beSome(contain(exactly("test1", "test2", "test3")))
          }
        }.unsafeRunSync()
      }

    }
  }
}

trait EventStorageFilteredEventsSpec {
  self: Specification =>

  import InMemoryStorageSpec._

  def filteredEventsTests(storageResource: Resource[IO, EventStorage], storageName: String): Fragment = {
    s"$storageName getFilteredEvents" >> {
      "should return all events when no filters are applied" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
            request = EventsRequest(List.empty, None, None, None, 1, 10)
            result <- storage.getFilteredEvents(request)
          } yield {
            result.events must have size(2)
            result.totalItems must_== 2
            result.totalPages must_== 1
          }
        }.unsafeRunSync()
      }

      "should filter by validEvents flag" >> {
        val incompleteEvent = GoodEvent2.copy(incomplete = true)
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, incompleteEvent))
            validOnlyRequest = EventsRequest(List.empty, Some(true), None, None, 1, 10)
            failedOnlyRequest = EventsRequest(List.empty, Some(false), None, None, 1, 10)
            validResult <- storage.getFilteredEvents(validOnlyRequest)
            failedResult <- storage.getFilteredEvents(failedOnlyRequest)
          } yield {
            validResult.events must have size(1)
            validResult.totalItems must_== 1

            failedResult.events must have size(1)
            failedResult.totalItems must_== 1
          }
        }.unsafeRunSync()
      }

      "should filter by time range" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            startTime = GoodEvent1.event.collector_tstamp
            endTime = GoodEvent3.event.collector_tstamp
            timeRange = TimeRange(Some(startTime), Some(endTime))
            request = EventsRequest(List.empty, None, Some(timeRange), None, 1, 10)
            result <- storage.getFilteredEvents(request)
          } yield {
            result.events must have size(2) // Should exclude Event3
            result.totalItems must_== 2
          }
        }.unsafeRunSync()
      }

      "should filter by simple column values" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
            filter = EventsFilter("app_id", "test1")
            request = EventsRequest(List(filter), None, None, None, 1, 10)
            result <- storage.getFilteredEvents(request)
          } yield {
            result.events must have size(1)
            result.totalItems must_== 1
          }
        }.unsafeRunSync()
      }

      "should ignore complex column filters (contexts_, unstruct_event_)" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
            contextFilter = EventsFilter("contexts_some_field", "test")
            unstructFilter = EventsFilter("unstruct_event_some_field", "test")
            request = EventsRequest(List(contextFilter, unstructFilter), None, None, None, 1, 10)
            result <- storage.getFilteredEvents(request)
          } yield {
            // Should return all events since complex filters are ignored
            result.events must have size(2)
            result.totalItems must_== 2
          }
        }.unsafeRunSync()
      }

      "should sort events by column" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
            sortingAsc = EventsSorting("app_id", false)
            sortingDesc = EventsSorting("app_id", true)
            requestAsc = EventsRequest(List.empty, None, None, Some(sortingAsc), 1, 10)
            requestDesc = EventsRequest(List.empty, None, None, Some(sortingDesc), 1, 10)
            resultAsc <- storage.getFilteredEvents(requestAsc)
            resultDesc <- storage.getFilteredEvents(requestDesc)
          } yield {
            // Events should be sorted differently
            resultAsc.events must have size(2)
            resultDesc.events must have size(2)
            resultAsc.events.head must_!= resultDesc.events.head
          }
        }.unsafeRunSync()
      }

      "should support pagination" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            page1Request = EventsRequest(List.empty, None, None, None, 1, 2)
            page2Request = EventsRequest(List.empty, None, None, None, 2, 2)
            page1Result <- storage.getFilteredEvents(page1Request)
            page2Result <- storage.getFilteredEvents(page2Request)
          } yield {
            page1Result.events must have size(2)
            page1Result.totalItems must_== 3
            page1Result.totalPages must_== 2

            page2Result.events must have size(1)
            page2Result.totalItems must_== 3
            page2Result.totalPages must_== 2
          }
        }.unsafeRunSync()
      }

      "should handle empty filter values" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2))
            emptyFilter = EventsFilter("app_id", "")
            request = EventsRequest(List(emptyFilter), None, None, None, 1, 10)
            result <- storage.getFilteredEvents(request)
          } yield {
            // Empty filter value should return all events
            result.events must have size(2)
            result.totalItems must_== 2
          }
        }.unsafeRunSync()
      }

      "should handle filtering" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1.copy(event = GoodEvent1.event.copy(app_id = Some("testapp")))))
            matchFilter = EventsFilter("app_id", "testapp")
            noMatchFilter = EventsFilter("app_id", "other")
            matchRequest = EventsRequest(List(matchFilter), None, None, None, 1, 10)
            noMatchRequest = EventsRequest(List(noMatchFilter), None, None, None, 1, 10)
            matchResult <- storage.getFilteredEvents(matchRequest)
            noMatchResult <- storage.getFilteredEvents(noMatchRequest)
          } yield {
            matchResult.events must have size(1)
            noMatchResult.events must have size(0)
          }
        }.unsafeRunSync()
      }

      "should combine multiple filters" >> {
        val event1Modified = GoodEvent1.copy(event = GoodEvent1.event.copy(app_id = Some("app1"), event_name = Some("click")))
        val event2Modified = GoodEvent2.copy(event = GoodEvent2.event.copy(app_id = Some("app1"), event_name = Some("view")))
        val event3Modified = GoodEvent3.copy(event = GoodEvent3.event.copy(app_id = Some("app2"), event_name = Some("click")))

        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(event1Modified, event2Modified, event3Modified))
            appFilter = EventsFilter("app_id", "app1")
            eventFilter = EventsFilter("event_name", "click")
            request = EventsRequest(List(appFilter, eventFilter), None, None, None, 1, 10)
            result <- storage.getFilteredEvents(request)
          } yield {
            // Should only match event1Modified (app1 AND click)
            result.events must have size(1)
            result.totalItems must_== 1
          }
        }.unsafeRunSync()
      }
    }
  }
}