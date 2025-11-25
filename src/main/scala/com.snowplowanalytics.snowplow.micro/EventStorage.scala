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
import com.snowplowanalytics.snowplow.micro.Configuration.StorageConfig
import io.circe.Json

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.collection.mutable

trait EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit]
  def addToBad(events: List[BadEvent]): IO[Unit]
  def reset(): IO[Unit]
  def getColumns: IO[List[String]]
  def getTimeline: IO[TimelineData]
  def getColumnStats(columns: List[String]): IO[ColumnStatsResponse]
  def getFilteredEvents(request: EventsRequest): IO[EventsResponse]
}

object NoStorage extends EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit] = IO.unit
  def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit
  def reset(): IO[Unit] = IO.unit
  def getColumns: IO[List[String]] = IO.pure(List.empty)
  def getTimeline: IO[TimelineData] = IO.pure(TimelineData(List.empty))
  def getColumnStats(columns: List[String]): IO[ColumnStatsResponse] = IO.pure(ColumnStatsResponse(Map.empty, None))
  def getFilteredEvents(request: EventsRequest): IO[EventsResponse] = IO.pure(EventsResponse(List.empty, 0, 0))
}

object EventStorage {
  def create(config: StorageConfig): Resource[IO, EventStorage] = {
    config match {
      case StorageConfig.None =>
        Resource.pure(NoStorage)
      case StorageConfig.Persistent(path, _, _) =>
        SqliteStorage.file(path.toString)
      case StorageConfig.InMemory =>
        Resource.pure(new InMemoryStorage())
    }
  }

  def fillMissingMinutes(points: List[TimelinePoint]): List[TimelinePoint] = {
    // the first point is the latest one
    val latestTime = points.headOption.fold(Instant.now().truncatedTo(ChronoUnit.MINUTES))(_.timestamp)
    val pointMap = points.map(p => p.timestamp -> p).toMap

    (0L to 30L).toList.map { delta =>
      val minute = latestTime.minus(delta, ChronoUnit.MINUTES)
      pointMap.getOrElse(minute, TimelinePoint(0, 0, minute))
    }
  }

  def isTimestampColumn(column: String): Boolean = column.endsWith("_tstamp")
  def isContextsColumn(column: String): Boolean = column.startsWith("contexts_")
  def isUnstructEventColumn(column: String): Boolean = column.startsWith("unstruct_event")
  def isComplexColumn(column: String): Boolean = isContextsColumn(column) || isUnstructEventColumn(column)

  def extractColumnsFromEvent(eventJson: Json): Set[String] = {
    val columnNames = mutable.Set[String]()

    eventJson.asObject.foreach { obj =>
      columnNames ++= obj.keys

      obj.toIterable.foreach { case (fieldName, value) =>
        if (isContextsColumn(fieldName)) {
          value.asArray.foreach { arr =>
            arr.foreach { item =>
              item.asObject.foreach { obj =>
                columnNames ++= obj.keys.map(key => s"$fieldName.$key")
              }
            }
          }
        }
        else if (isUnstructEventColumn(fieldName)) {
          value.asObject.foreach { obj =>
            columnNames ++= obj.keys.map(key => s"$fieldName.$key")
          }
        }
      }
    }

    columnNames.toSet
  }
}
