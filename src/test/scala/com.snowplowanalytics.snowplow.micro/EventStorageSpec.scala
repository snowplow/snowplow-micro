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