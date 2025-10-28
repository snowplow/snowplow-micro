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
import scala.util.Random

/** SQLite-based event storage that only stores good events for the /micro/events endpoint.
  * Ignores bad events and only stores the JSON representation of events.
  */
private[micro] class SqliteStorage(xa: Transactor[IO], maxEvents: Option[Int]) extends EventStorage {

  /** Add good events to SQLite storage. Only stores the event field as JSON. */
  def addToGood(events: List[GoodEvent]): IO[Unit] = {
    if (events.nonEmpty) {
      val inserts = events.map { goodEvent =>
        val timestamp = Timestamp.from(goodEvent.event.collector_tstamp)
        val eventId = goodEvent.event.event_id.toString
        val eventJson = goodEvent.event.toJson(lossy = true).noSpaces
        sql"INSERT OR IGNORE INTO events (event_id, timestamp, event_json) VALUES ($eventId, $timestamp, $eventJson)"
      }

      val insertProgram = inserts.traverse(_.update.run).void
      val cleanupProgram = maxEvents.fold(().pure[ConnectionIO]) { limit =>
        // Only run cleanup with 1% probability to avoid excessive overhead
        if (Random.nextDouble() < 0.01) {
          cleanupOldEvents(limit)
        } else {
          ().pure[ConnectionIO]
        }
      }

      (insertProgram *> cleanupProgram).transact(xa)
    } else {
      IO.unit
    }
  }

  /** Add bad events - ignored in SQLite storage. */
  def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit

  /** Reset storage by deleting all events. */
  def reset(): IO[Unit] = {
    sql"DELETE FROM events".update.run.transact(xa).void
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

  /** Initialize the storage by creating the table if it doesn't exist. */
  def initialize(): IO[Unit] = createTableIfNotExists.transact(xa)

  private def createTableIfNotExists: ConnectionIO[Unit] = {
    sql"""
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        event_json JSON NOT NULL
      )
    """.update.run.void *>
    sql"CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)".update.run.void
  }

  private def cleanupOldEvents(limit: Int): ConnectionIO[Unit] = {
    sql"""
      DELETE FROM events
      WHERE event_id NOT IN (
        SELECT event_id FROM events
        ORDER BY timestamp DESC
        LIMIT $limit
      )
    """.update.run.void
  }
}

private[micro] object SqliteStorage {

  /** Create SQLite storage with file-based database. */
  def file(dbPath: String, maxEvents: Option[Int]): Resource[IO, SqliteStorage] = {
    val url = s"jdbc:sqlite:$dbPath"
    createTransactor(url, maxEvents)
  }

  /** Create SQLite storage with in-memory database. */
  def inMemory(maxEvents: Option[Int]): Resource[IO, SqliteStorage] = {
    val url = "jdbc:sqlite::memory:"
    createTransactor(url, maxEvents)
  }

  private def createTransactor(url: String, maxEvents: Option[Int]): Resource[IO, SqliteStorage] = {
    HikariTransactor.newHikariTransactor[IO](
      driverClassName = "org.sqlite.JDBC",
      url = url,
      user = "", // SQLite doesn't use user/password
      pass = "",
      connectEC = scala.concurrent.ExecutionContext.global
    ).evalTap { xa =>
      // Initialize the database schema
      val storage = new SqliteStorage(xa, maxEvents)
      storage.initialize()
    }.map(xa => new SqliteStorage(xa, maxEvents))
  }
}