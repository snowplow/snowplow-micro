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

            val eventMinute12 = EventStorage.roundToMinute(GoodEvent1.event.collector_tstamp.toEpochMilli)
            val eventMinute3 = EventStorage.roundToMinute(GoodEvent3.event.collector_tstamp.toEpochMilli)

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

            val eventMinute12 = EventStorage.roundToMinute(GoodEvent1.event.collector_tstamp.toEpochMilli)
            val eventMinute3 = EventStorage.roundToMinute(failedEvent.event.collector_tstamp.toEpochMilli)

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
            timeline.points.map(_.timestamp) must beSorted(Ordering.Long.reverse)
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
      "should return empty map for empty storage" >> {
        storageResource.use { storage =>
          storage.getColumnStats(List("event_id", "app_id")).map { stats =>
            stats must be empty
          }
        }.unsafeRunSync()
      }

      "should return distinct values for simple columns" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            stats <- storage.getColumnStats(List("event_id", "app_id"))
          } yield {
            stats must haveKey("event_id")
            stats must haveKey("app_id")

            stats("event_id").values must contain(exactly(
              GoodEvent1.event.event_id.toString,
              GoodEvent2.event.event_id.toString,
              GoodEvent3.event.event_id.toString
            ))

            stats("app_id").values must contain(exactly("test1", "test2", "test3"))
          }
        }.unsafeRunSync()
      }

      "should ignore complex columns (containing dots)" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1))
            stats <- storage.getColumnStats(List("event_id", "contexts_com_example_schema_1.field1"))
          } yield {
            stats must haveKey("event_id")
            stats must not(haveKey("contexts_com_example_schema_1.field1"))
          }
        }.unsafeRunSync()
      }

      "should ignore timestamp columns" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1))
            stats <- storage.getColumnStats(List("event_id", "collector_tstamp", "derived_tstamp"))
          } yield {
            stats must haveKey("event_id")
            stats must not(haveKey("collector_tstamp"))
            stats must not(haveKey("derived_tstamp"))
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
            stats <- storage.getColumnStats(List("event_id"))
          } yield {
            stats must haveKey("event_id")
            stats("event_id").values must have size(20)
          }
        }.unsafeRunSync()
      }

      "should return stats for JSON-extracted columns" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1, GoodEvent2, GoodEvent3))
            stats <- storage.getColumnStats(List("v_collector"))
          } yield {
            stats must haveKey("v_collector")
            stats("v_collector").values must contain(exactly("collector1"))
          }
        }.unsafeRunSync()
      }

      "should return empty stats for non-existent columns" >> {
        storageResource.use { storage =>
          for {
            _ <- storage.addToGood(List(GoodEvent1))
            stats <- storage.getColumnStats(List("non_existent_column"))
          } yield {
            stats must be empty
          }
        }.unsafeRunSync()
      }
    }
  }
}