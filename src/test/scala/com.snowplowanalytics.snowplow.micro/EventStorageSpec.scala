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
import org.specs2.mutable.Specification
import io.circe.Json
import io.circe.parser.parse

class EventStorageSpec extends Specification {

  class TestEventStorage extends EventStorage {
    def addToGood(events: List[GoodEvent]): IO[Unit] = IO.unit
    def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit
    def reset(): IO[Unit] = IO.unit
    def getEvents: IO[List[Json]] = IO.pure(List.empty)
    def getColumns: IO[List[String]] = IO.pure(List.empty)
    def getTimeline: IO[TimelineData] = IO.pure(TimelineData(List.empty))

    def testExtractColumnsFromEvent(eventJson: Json): Set[String] = {
      extractColumnsFromEvent(eventJson)
    }
  }

  val testStorage = new TestEventStorage()

  "extractColumnsFromEvent" >> {
    "should extract top-level column names" >> {
      val json = parse("""
        {
          "event_id": "123",
          "collector_tstamp": "2023-01-01T00:00:00Z",
          "app_id": "test-app"
        }
      """).getOrElse(Json.Null)

      val columns = testStorage.testExtractColumnsFromEvent(json)
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

      val columns = testStorage.testExtractColumnsFromEvent(json)
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

      val columns = testStorage.testExtractColumnsFromEvent(json)
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

      val columns = testStorage.testExtractColumnsFromEvent(json)
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

      val columns = testStorage.testExtractColumnsFromEvent(json)
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

      val columns = testStorage.testExtractColumnsFromEvent(json)
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

      val columns = testStorage.testExtractColumnsFromEvent(json)
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
      val columns = testStorage.testExtractColumnsFromEvent(json)
      columns must be empty
    }
  }
}