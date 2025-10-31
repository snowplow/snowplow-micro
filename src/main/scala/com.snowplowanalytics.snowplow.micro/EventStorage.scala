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
import com.snowplowanalytics.snowplow.micro.Configuration.MicroConfig
import io.circe.Json
import scala.collection.mutable

trait EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit]
  def addToBad(events: List[BadEvent]): IO[Unit]
  def reset(): IO[Unit]
  def getEvents: IO[List[Json]]
  def getColumns: IO[List[String]]
  def getTimeline: IO[TimelineData]
  def getColumnStats(columns: List[String]): IO[Map[String, ColumnStats]]
}

object NoStorage extends EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit] = IO.unit
  def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit
  def reset(): IO[Unit] = IO.unit
  def getEvents: IO[List[Json]] = IO.pure(List.empty)
  def getColumns: IO[List[String]] = IO.pure(List.empty)
  def getTimeline: IO[TimelineData] = IO.pure(TimelineData(List.empty))
  def getColumnStats(columns: List[String]): IO[Map[String, ColumnStats]] = IO.pure(Map.empty)
}

object EventStorage {
  def create(config: MicroConfig): Resource[IO, EventStorage] = {
    (config.storage, config.maxEvents) match {
      case (_, Some(0)) =>
        Resource.pure(NoStorage)
      case (Some(storagePath), maxEvents) =>
        SqliteStorage.file(storagePath.toString, maxEvents)
      case (None, Some(maxEvents)) =>
        SqliteStorage.inMemory(Some(maxEvents))
      case (None, None) =>
        Resource.pure(new InMemoryStorage())
    }
  }

  def roundToMinute(timestamp: Long): Long = {
    (timestamp / 60000) * 60000
  }

  def fillMissingMinutes(points: List[TimelinePoint]): List[TimelinePoint] = {
    // the first point is the latest one
    val latestTime = points.headOption.fold(roundToMinute(System.currentTimeMillis()))(_.timestamp)
    val startTime = latestTime - 30 * 60000
    val pointMap = points.map(p => p.timestamp -> p).toMap

    (latestTime to startTime by -60000).toList.map { minute =>
      pointMap.getOrElse(minute, TimelinePoint(0, 0, minute))
    }
  }

  def extractColumnsFromEvent(eventJson: Json): Set[String] = {
    val columnNames = mutable.Set[String]()

    eventJson.asObject.foreach { obj =>
      columnNames ++= obj.keys

      obj.toIterable.foreach { case (fieldName, value) =>
        if (fieldName.startsWith("contexts_")) {
          value.asArray.foreach { arr =>
            arr.foreach { item =>
              item.asObject.foreach { obj =>
                columnNames ++= obj.keys.map(key => s"$fieldName.$key")
              }
            }
          }
        }
        else if (fieldName.startsWith("unstruct_event_")) {
          value.asObject.foreach { obj =>
            columnNames ++= obj.keys.map(key => s"$fieldName.$key")
          }
        }
      }
    }

    columnNames.toSet
  }
}