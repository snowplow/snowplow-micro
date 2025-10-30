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
import doobie._
import doobie.implicits._
import doobie.hikari.HikariTransactor
import io.circe.{Json, parser}

import java.sql.Timestamp
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.util.Random

/** SQLite-based event storage.
 * Only stores the Analytics SDK JSON representation of valid and failed (incomplete) events.
  */
private[micro] class SqliteStorage(xa: Transactor[IO], maxEvents: Option[Int]) extends EventStorage {
  import SqliteStorage._

  /** Add events to SQLite storage. */
  override def addToGood(events: List[GoodEvent]): IO[Unit] = {
    if (events.nonEmpty) {
      val eventJsons = events.map(_.event.toJson(lossy = true))

      val eventData = events.zip(eventJsons).map { case (goodEvent, eventJson) =>
        val appId = goodEvent.event.app_id
        val eventName = goodEvent.event.event_name
        (goodEvent.event.event_id.toString, Timestamp.from(goodEvent.event.collector_tstamp), eventJson.noSpaces, goodEvent.incomplete, appId, eventName)
      }

      val allColumns = eventJsons.map(EventStorage.extractColumnsFromEvent)
        .reduce(_.union(_)).toList

      val insertEventsProgram = {
        val sql = "INSERT OR IGNORE INTO events (event_id, timestamp, event_json, failed, app_id, event_name) VALUES (?, ?, ?, ?, ?, ?)"
        Update[(String, Timestamp, String, Boolean, Option[String], Option[String])](sql).updateMany(eventData)
      }

      val insertColumnsProgram = if (allColumns.nonEmpty) {
        val sql = "INSERT OR IGNORE INTO columns (name) VALUES (?)"
        Update[String](sql).updateMany(allColumns)
      } else {
        ().pure[ConnectionIO]
      }

      val cleanupProgram = maxEvents.fold(().pure[ConnectionIO]) { limit =>
        // Only run cleanup with 1% probability to avoid excessive overhead
        if (Random.nextDouble() < 0.01) {
          cleanupOldEvents(limit)
        } else {
          ().pure[ConnectionIO]
        }
      }

      (insertEventsProgram *> insertColumnsProgram *> cleanupProgram).transact(xa).void
    } else {
      IO.unit
    }
  }

  /** Add bad events - ignored in SQLite storage. */
  override def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit

  /** Reset storage by deleting all events. */
  def reset(): IO[Unit] = {
    (sql"DELETE FROM events".update.run *> sql"DELETE FROM columns".update.run).transact(xa).void
  }

  /** Get all events as JSON for the /micro/events endpoint. */
  def getEvents: IO[List[Json]] = {
    val query = maxEvents match {
      case Some(limit) => sql"SELECT event_json FROM events ORDER BY timestamp DESC LIMIT $limit"
      case None => sql"SELECT event_json FROM events ORDER BY timestamp DESC"
    }

    query
      .query[String]
      .to[List]
      .transact(xa)
      .flatMap { jsonStrings =>
        jsonStrings.traverse { jsonStr =>
          IO.fromEither(parser.parse(jsonStr))
        }
      }
  }

  def getColumns: IO[List[String]] = {
    sql"SELECT name FROM columns ORDER BY name"
      .query[String]
      .to[List]
      .transact(xa)
  }

  def getTimeline: IO[TimelineData] = {
    val query = sql"""
      WITH latest_event AS (
        SELECT COALESCE(MAX(timestamp), ${System.currentTimeMillis()}) as max_timestamp FROM events
      ),
      time_range AS (
        SELECT
          (max_timestamp / 60000) * 60000 as latest_minute,
          (max_timestamp / 60000) * 60000 - 30 * 60000 as start_minute
        FROM latest_event
      ),
      sparse_data AS (
        SELECT
          (timestamp / 60000) * 60000 as minute,
          COUNT(CASE WHEN NOT failed THEN 1 END) as valid_count,
          COUNT(CASE WHEN failed THEN 1 END) as failed_count
        FROM events, time_range
        WHERE timestamp >= time_range.start_minute AND timestamp < (time_range.latest_minute + 60000)
        GROUP BY minute
      )
      SELECT minute, valid_count, failed_count FROM sparse_data ORDER BY minute DESC
    """

    query
      .query[TimelinePoint]
      .to[List]
      .transact(xa)
      .map { sparsePoints =>
        val filledPoints = EventStorage.fillMissingMinutes(sparsePoints)
        TimelineData(filledPoints)
      }
  }
}

private[micro] object SqliteStorage {
  // few threads since SQLite does not support multiple concurrent writes anyway
  private val databaseExecutionContext = ExecutionContext
    .fromExecutor(Executors.newFixedThreadPool(2))

  /** Create SQLite storage with file-based database. */
  def file(dbPath: String, maxEvents: Option[Int]): Resource[IO, SqliteStorage] = {
    val url = s"jdbc:sqlite:$dbPath"
    create(url, maxEvents)
  }

  /** Create SQLite storage with in-memory database. */
  def inMemory(maxEvents: Option[Int]): Resource[IO, SqliteStorage] = {
    val url = "jdbc:sqlite::memory:"
    create(url, maxEvents)
  }

  private def create(url: String, maxEvents: Option[Int]): Resource[IO, SqliteStorage] = {
    for {
      xa <- HikariTransactor.newHikariTransactor[IO](
        driverClassName = "org.sqlite.JDBC",
        url = url,
        user = "", // SQLite doesn't use user/password
        pass = "",
        connectEC = databaseExecutionContext
      )
      _ <- Resource.eval(initialize(xa))
    } yield new SqliteStorage(xa, maxEvents)
  }

  private def initialize(xa: Transactor[IO]): IO[Unit] =
    (createEventsTable *> createColumnsTable).transact(xa)

  private def createEventsTable: ConnectionIO[Unit] = {
    sql"""
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        event_json JSON NOT NULL,
        failed BOOLEAN NOT NULL,
        app_id TEXT,
        event_name TEXT
      )
    """.update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_app_id ON events(app_id)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_event_name ON events(event_name)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_failed ON events(failed)".update.run.void
  }

  private def createColumnsTable: ConnectionIO[Unit] = {
    sql"""
      CREATE TABLE IF NOT EXISTS columns (
        name TEXT PRIMARY KEY
      )
    """.update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_columns_name ON columns(name)".update.run.void
  }

  private def cleanupOldEvents(limit: Int): ConnectionIO[Unit] = {
    sql"""
        DELETE FROM events
        WHERE timestamp < (
          SELECT timestamp
          FROM events
          ORDER BY timestamp DESC
          LIMIT 1 OFFSET $limit
        )
      """.update.run.void
  }

  implicit val timelinePointRead: Read[TimelinePoint] =
    Read[(Long, Int, Int)].map { case (timestamp, validEvents, failedEvents) =>
      TimelinePoint(validEvents, failedEvents, timestamp)
    }
}