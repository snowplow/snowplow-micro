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
import cats.implicits._
import com.snowplowanalytics.snowplow.micro.Configuration.StorageMode
import com.snowplowanalytics.snowplow.micro.model.ColumnStatsResponse
import io.circe.Json
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.collection.mutable

trait EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit]
  def addToBad(events: List[BadEvent]): IO[Unit]
  def reset(): IO[Unit]
  def getColumns: IO[List[String]]
  def getTimeline(request: TimelineRequest): IO[TimelineData]
  def getColumnStats(columns: List[String]): IO[ColumnStatsResponse]
  def getFilteredEvents(request: EventsRequest): IO[EventsResponse]
}

object NoStorage extends EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit] = IO.unit
  def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit
  def reset(): IO[Unit] = IO.unit
  def getColumns: IO[List[String]] = IO.pure(List.empty)
  def getTimeline(request: TimelineRequest): IO[TimelineData] = {
    val emptyPoints = request.buckets.map(bucket => TimelinePoint(0, 0, bucket))
    IO.pure(TimelineData(emptyPoints))
  }
  def getColumnStats(columns: List[String]): IO[ColumnStatsResponse] = IO.pure(Map.empty)
  def getFilteredEvents(request: EventsRequest): IO[EventsResponse] = IO.pure(EventsResponse(List.empty, 0, 0))
}

object EventStorage {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def create(config: StorageMode): Resource[IO, EventStorage] = {
    config match {
      case StorageMode.None =>
        Resource.eval(logger.info("Not storing any events")) *>
        Resource.pure(NoStorage)
      case StorageMode.Persistent(host, port, database, user, password, ttl, cleanupInterval) =>
        Resource.eval(logger.info(s"Storing events in PostgreSQL at $host:$port/$database")) *>
        (for {
          storage <- PostgresqlStorage.create(host, port, database, user, password)
          _ <- storage.scheduleCleanup(ttl, cleanupInterval)
        } yield storage)
      case StorageMode.InMemory =>
        Resource.eval(logger.info("Storing events in memory")) *>
        Resource.pure(new InMemoryStorage())
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
