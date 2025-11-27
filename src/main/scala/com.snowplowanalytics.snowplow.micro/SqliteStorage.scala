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
import com.snowplowanalytics.snowplow.micro.model.ColumnStatsResponse
import doobie._
import doobie.implicits._
import doobie.hikari.HikariTransactor
import fs2.Stream
import io.circe.parser
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.sql.Timestamp
import java.time.Instant
import scala.concurrent.duration._

/** SQLite-based event storage.
 * Only stores the Analytics SDK JSON representation of valid and failed (incomplete) events.
 * Uses separate transactors for reads and writes to allow concurrent access in WAL mode.
  */
private[micro] class SqliteStorage(readXa: Transactor[IO], writeXa: Transactor[IO]) extends EventStorage {
  import SqliteStorage._

  /** Add events to SQLite storage. */
  override def addToGood(events: List[GoodEvent]): IO[Unit] = {
    if (events.nonEmpty) {
      val eventJsons = events.map(_.event.toJson(lossy = true))

      val eventData = events.zip(eventJsons).map { case (goodEvent, eventJson) =>
        val appId = goodEvent.event.app_id
        val eventName = goodEvent.event.event_name
        val platform = goodEvent.event.platform
        val nameTracker = goodEvent.event.name_tracker
        val domainUserid = goodEvent.event.domain_userid
        val vTracker = goodEvent.event.v_tracker
        (goodEvent.event.event_id.toString, goodEvent.event.collector_tstamp, eventJson.noSpaces, goodEvent.incomplete, appId, eventName, platform, nameTracker, domainUserid, vTracker)
      }

      val allColumns = eventJsons.map(EventStorage.extractColumnsFromEvent)
        .reduce(_.union(_)).toList

      val insertEventsProgram = {
        val sql = "INSERT OR IGNORE INTO events (event_id, timestamp, event_json, failed, app_id, event_name, platform, name_tracker, domain_userid, v_tracker) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        Update[(String, Instant, String, Boolean, Option[String], Option[String], Option[String], Option[String], Option[String], Option[String])](sql).updateMany(eventData)
      }

      val insertColumnsProgram = if (allColumns.nonEmpty) {
        val sql = "INSERT OR IGNORE INTO columns (name) VALUES (?)"
        Update[String](sql).updateMany(allColumns)
      } else {
        ().pure[ConnectionIO]
      }

      (insertEventsProgram *> insertColumnsProgram).transact(writeXa).void
    } else {
      IO.unit
    }
  }

  /** Add bad events - ignored in SQLite storage. */
  override def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit

  /** Reset storage by deleting all events. */
  override def reset(): IO[Unit] = {
    (sql"DELETE FROM events".update.run *> sql"DELETE FROM columns".update.run).transact(writeXa).void
  }

  override def getColumns: IO[List[String]] = {
    sql"SELECT name FROM columns ORDER BY name"
      .query[String]
      .to[List]
      .transact(readXa)
  }

  override def getTimeline: IO[TimelineData] = {
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
      .transact(readXa)
      .map { sparsePoints =>
        val filledPoints = EventStorage.fillMissingMinutes(sparsePoints)
        TimelineData(filledPoints)
      }
  }

  override def getColumnStats(columns: List[String]): IO[ColumnStatsResponse] = {

    columns.parTraverse { column =>
      val sortable = column === "collector_tstamp" || INDEXED_COLUMNS.contains(column)
      val filterable = INDEXED_COLUMNS.contains(column)

      val distinctValues = if (filterable) {
        val frCol = Fragment.const(column)
        val query = fr"""
          | SELECT DISTINCT value FROM (
          |   SELECT $frCol AS value FROM events
          |   ORDER BY timestamp DESC LIMIT $COLUMN_STATS_SCAN_LIMIT
          | ) WHERE value IS NOT NULL LIMIT 20
          |""".stripMargin

        query.query[String]
          .to[List]
          .transact(readXa)
          .map(values => Some(values))
      } else {
        IO.pure(None)
      }

      distinctValues.map { values =>
        column -> ColumnStats(
          sortable = sortable,
          filterable = filterable,
          values = values
        )
      }
    }.map(_.toMap)
  }

  override def getFilteredEvents(request: EventsRequest): IO[EventsResponse] = {
    val baseConditions = fr"WHERE 1=1"

    // Build WHERE conditions
    val whereConditions = List(
      // Valid events filter
      request.validEvents.map { validOnly =>
        fr"AND failed = ${!validOnly}"
      },
      // Time range filter
      request.timeRange.flatMap(_.start).map { start =>
        fr"AND timestamp >= $start"
      },
      request.timeRange.flatMap(_.end).map { end =>
        fr"AND timestamp < $end"
      }
    ).flatten

    // Only filter on indexed columns (SQLite mode doesn't support JSON column filtering)
    val columnFilters = request.filters
      .filter(f => INDEXED_COLUMNS.contains(f.column))
      .filter(_.value.nonEmpty) // Skip empty filter values
      .map { filter =>
        val frCol = Fragment.const(filter.column)
        fr"AND $frCol = ${filter.value}"
      }

    val allConditions = whereConditions ++ columnFilters
    val whereClause = allConditions.foldLeft(baseConditions)(_ ++ _)

    // Only allow sorting on indexed columns (SQLite mode doesn't support JSON column sorting)
    val orderByClause = request.sorting.filter { s =>
        s.column === "collector_tstamp" || INDEXED_COLUMNS.contains(s.column)
      }.fold(fr"ORDER BY timestamp DESC") { sorting =>
        val columnExpr = if (sorting.column == "collector_tstamp") {
          Fragment.const("timestamp")
        } else {
          Fragment.const(sorting.column)
        }
        val direction = Fragment.const(if (sorting.desc) "DESC" else "ASC")
        fr"ORDER BY $columnExpr $direction"
      }

    // prevent OOM when an unreasonably large page size is supplied
    // the UI only uses 50 currently
    val safePageSize = Math.max(Math.min(request.pageSize, 100), 1)
    val offset = (request.page - 1) * safePageSize

    val countQuery = fr"SELECT COUNT(*) FROM events $whereClause"
    val dataQuery = fr"""
      | SELECT event_json FROM events
      | $whereClause $orderByClause
      | LIMIT $safePageSize OFFSET $offset
      |""".stripMargin

    (
      countQuery.query[Int].unique.transact(readXa),
      dataQuery.query[String].to[List].transact(readXa)
    ).parTupled.flatMap { case (totalItems, jsonStrings) =>
      jsonStrings.traverse { jsonStr =>
        IO.fromEither(parser.parse(jsonStr))
      }.map { events =>
        val totalPages = Math.max(1, (totalItems + safePageSize - 1) / safePageSize)
        EventsResponse(events, totalPages, totalItems)
      }
    }
  }

  def cleanupExpiredEvents(ttl: FiniteDuration): IO[Unit] =
    for {
      now <- IO.realTimeInstant
      cutoffTime = now.minusMillis(ttl.toMillis)
      _ <- logger.info(s"Running TTL cleanup: deleting events older than $cutoffTime (TTL: $ttl)")
      deletedCount <- sql"DELETE FROM events WHERE timestamp < $cutoffTime".update.run.transact(writeXa)
      _ <- logger.info(s"TTL cleanup completed: deleted $deletedCount events")
    } yield ()

  def scheduleCleanup(ttl: FiniteDuration, interval: FiniteDuration): Resource[IO, Unit] = {
    Resource.eval(cleanupExpiredEvents(ttl)) *>
    scheduleBackgroundTask(cleanupExpiredEvents(ttl), interval)
  }
}

private[micro] object SqliteStorage {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // Columns that have dedicated table columns (rather than JSON extraction)
  private val INDEXED_COLUMNS = Set("event_id", "app_id", "event_name", "platform", "name_tracker", "domain_userid", "v_tracker")

  // Limit scan depth for columnStats queries to improve performance
  // Only scan most recent N events instead of entire table
  // Tunable: increase for more coverage, decrease for better performance
  private val COLUMN_STATS_SCAN_LIMIT = 1000

  /** Create SQLite storage with file-based database. */
  def file(dbPath: String): Resource[IO, SqliteStorage] = {
    val params = List(
      "journal_mode=WAL",   // Write-Ahead Logging for better concurrency
      "busy_timeout=30000", // Wait up to 30 seconds instead of immediate BUSY error
      "synchronous=FULL",   // Maximum durability - fsync after every commit
      "cache_size=-20000",  // 20MB cache (negative = KB)
      "temp_store=MEMORY"   // Use memory for temporary tables
    )
    val url = s"jdbc:sqlite:$dbPath?${params.mkString("&")}"
    for {
      // Write transactor: single connection (SQLite only allows 1 writer anyway)
      writeXa <- transactor(url, maxPoolSize = 1)

      // Read transactor: multiple connections for concurrent reads (WAL mode supports this)
      readXa <- transactor(url, maxPoolSize = 4)

      _ <- Resource.eval(initialize(writeXa))
    } yield new SqliteStorage(readXa, writeXa)
  }

  private def transactor(url: String, maxPoolSize: Int): Resource[IO, HikariTransactor[IO]] =
    for {
      ec <- ExecutionContexts.fixedThreadPool[IO](maxPoolSize)
      xa <- HikariTransactor.newHikariTransactor[IO](
        driverClassName = "org.sqlite.JDBC",
        url = url,
        user = "",
        pass = "",
        connectEC = ec
      )
      _ <- Resource.eval(configureDataSource(xa, maxPoolSize = maxPoolSize))
    } yield xa

  private def configureDataSource(xa: HikariTransactor[IO], maxPoolSize: Int): IO[Unit] = IO {
    val config = xa.kernel.getHikariConfigMXBean
    config.setMaximumPoolSize(maxPoolSize)
    config.setMinimumIdle(1)
    config.setConnectionTimeout(30000)  // 30 second connection timeout
    config.setIdleTimeout(600000)       // 10 minute idle timeout
  }

  private def initialize(xa: Transactor[IO]): IO[Unit] =
    (createEventsTable *> createColumnsTable *> analyzeDatabase).transact(xa)

  private def analyzeDatabase: ConnectionIO[Unit] =
    sql"ANALYZE".update.run.void

  private def createEventsTable: ConnectionIO[Unit] = {
    sql"""
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        timestamp TIMESTAMP NOT NULL,
        event_json JSON NOT NULL,
        failed BOOLEAN NOT NULL,
        app_id TEXT,
        event_name TEXT,
        platform TEXT,
        name_tracker TEXT,
        domain_userid TEXT,
        v_tracker TEXT
      )
    """.update.run.void *>
      // Timestamp-only index for queries without column filters
      sql"CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)".update.run.void *>
      // Composite indexes for filtered queries (column, timestamp)
      // These also serve single-column lookups via leftmost prefix
      sql"CREATE INDEX IF NOT EXISTS idx_events_failed_timestamp ON events(failed, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_app_id_timestamp ON events(app_id, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_event_name_timestamp ON events(event_name, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_platform_timestamp ON events(platform, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_name_tracker_timestamp ON events(name_tracker, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_domain_userid_timestamp ON events(domain_userid, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_v_tracker_timestamp ON events(v_tracker, timestamp)".update.run.void
  }

  private def createColumnsTable: ConnectionIO[Unit] = {
    sql"""
      CREATE TABLE IF NOT EXISTS columns (
        name TEXT PRIMARY KEY
      )
    """.update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_columns_name ON columns(name)".update.run.void
  }

  private def scheduleBackgroundTask(task: => IO[Unit], interval: FiniteDuration): Resource[IO, Unit] = {
    Stream
      .awakeDelay[IO](interval)
      .evalMap(_ => task)
      .compile
      .drain
      .background
      .void
  }

  implicit val instantEpochMeta: Meta[Instant] =
    Meta[Timestamp].timap(_.toInstant)(Timestamp.from)

  implicit val timelinePointRead: Read[TimelinePoint] =
    Read[(Instant, Int, Int)].map { case (timestamp, validEvents, failedEvents) =>
      TimelinePoint(validEvents, failedEvents, timestamp)
    }
}
