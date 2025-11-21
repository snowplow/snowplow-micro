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
import io.circe.parser
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.sql.Timestamp
import java.time.Instant
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

/** SQLite-based event storage.
 * Only stores the Analytics SDK JSON representation of valid and failed (incomplete) events.
  */
private[micro] class SqliteStorage(xa: Transactor[IO]) extends EventStorage {
  import SqliteStorage._

  /** Add events to SQLite storage. */
  override def addToGood(events: List[GoodEvent]): IO[Unit] = {
    if (events.nonEmpty) {
      val eventJsons = events.map(_.event.toJson(lossy = true))

      val eventData = events.zip(eventJsons).map { case (goodEvent, eventJson) =>
        val appId = goodEvent.event.app_id
        val eventName = goodEvent.event.event_name
        (goodEvent.event.event_id.toString, goodEvent.event.collector_tstamp, eventJson.noSpaces, goodEvent.incomplete, appId, eventName)
      }

      val allColumns = eventJsons.map(EventStorage.extractColumnsFromEvent)
        .reduce(_.union(_)).toList

      val insertEventsProgram = {
        val sql = "INSERT OR IGNORE INTO events (event_id, timestamp, event_json, failed, app_id, event_name) VALUES (?, ?, ?, ?, ?, ?)"
        Update[(String, Instant, String, Boolean, Option[String], Option[String])](sql).updateMany(eventData)
      }

      val insertColumnsProgram = if (allColumns.nonEmpty) {
        val sql = "INSERT OR IGNORE INTO columns (name) VALUES (?)"
        Update[String](sql).updateMany(allColumns)
      } else {
        ().pure[ConnectionIO]
      }

      (insertEventsProgram *> insertColumnsProgram).transact(xa).void
    } else {
      IO.unit
    }
  }

  /** Add bad events - ignored in SQLite storage. */
  override def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit

  /** Reset storage by deleting all events. */
  override def reset(): IO[Unit] = {
    (sql"DELETE FROM events".update.run *> sql"DELETE FROM columns".update.run).transact(xa).void
  }

  override def getColumns: IO[List[String]] = {
    sql"SELECT name FROM columns ORDER BY name"
      .query[String]
      .to[List]
      .transact(xa)
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
      .transact(xa)
      .map { sparsePoints =>
        val filledPoints = EventStorage.fillMissingMinutes(sparsePoints)
        TimelineData(filledPoints)
      }
  }

  override def getColumnStats(columns: List[String]): IO[Map[String, ColumnStats]] = {
    import SqliteStorage.COLUMN_STATS_SCAN_LIMIT

    // TODO: support complex columns at some point
    val simpleColumns = columns.filterNot(col =>
      EventStorage.isComplexColumn(col) ||
        EventStorage.isTimestampColumn(col)
    )

    simpleColumns.parTraverse { column =>
      val query = column match {
        case "event_id" | "app_id" | "event_name"  =>
          fr"SELECT DISTINCT value FROM (" ++
            fr"SELECT" ++ Fragment.const(column) ++ fr"as value FROM events" ++
            fr"ORDER BY timestamp DESC LIMIT ${COLUMN_STATS_SCAN_LIMIT}" ++
          fr") WHERE value IS NOT NULL LIMIT 20"
        case _ =>
          fr"SELECT DISTINCT CAST(json_extract(event_json, ${"$." + column}) AS TEXT) as value FROM (" ++
            fr"SELECT event_json FROM events ORDER BY timestamp DESC LIMIT ${COLUMN_STATS_SCAN_LIMIT}" ++
          fr") WHERE value IS NOT NULL AND value != 'null' LIMIT 20"
      }

      query.query[String]
        .to[List]
        .transact(xa)
        .map(values => column -> ColumnStats(values))
    }.map(_.filter(_._2.values.nonEmpty).toMap)
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

    // TODO: support complex columns at some point
    val columnFilters = request.filters
      .filterNot(f => EventStorage.isComplexColumn(f.column))
      .filter(_.value.nonEmpty) // Skip empty filter values
      .map { filter =>
        val equals = fr"=" ++ fr"${filter.value}" ++ fr"COLLATE NOCASE"
        filter.column match {
          case "event_id" | "app_id" | "event_name" =>
            fr"AND" ++ Fragment.const(filter.column) ++ equals
          case _ =>
            fr"AND CAST(json_extract(event_json, ${"$." + filter.column}) AS TEXT)" ++ equals
        }
      }

    val allConditions = whereConditions ++ columnFilters
    val whereClause = allConditions.foldLeft(baseConditions)(_ ++ _)

    // TODO: support complex columns at some point
    val orderByClause = request.sorting.filterNot(s =>
      EventStorage.isComplexColumn(s.column)
    ).fold(fr"ORDER BY timestamp DESC") { sorting =>
      val columnExpr = sorting.column match {
        case "collector_tstamp" =>
          Fragment.const("timestamp")
        case "event_id" | "app_id" | "event_name" =>
          Fragment.const(sorting.column)
        case _ =>
          fr"json_extract(event_json, ${"$." + sorting.column})"
      }
      val direction = if (sorting.desc) "DESC" else "ASC"
      fr"ORDER BY" ++ columnExpr ++ Fragment.const(s" $direction")
    }

    // prevent OOM when an unreasonably large page size is supplied
    // the UI only uses 50 currently
    val safePageSize = Math.max(Math.min(request.pageSize, 100), 1)

    // Split into two queries for better performance:
    // 1. Count query: Fast for indexed columns, approximate for JSON columns
    // 2. Data query: Processes only the requested page (e.g., 50 rows) instead of all rows
    val offset = (request.page - 1) * safePageSize

    val hasJsonFilter = request.filters.exists { filter =>
      filter.column match {
        case "event_id" | "app_id" | "event_name" => false
        case _ => true
      }
    }

    val countQuery = fr"SELECT COUNT(*) FROM events" ++ whereClause
    val dataQuery = fr"SELECT event_json FROM events" ++
      whereClause ++ orderByClause ++
      fr"LIMIT ${safePageSize} OFFSET ${offset}"

    val totalItemsIO: IO[(Int, Boolean)] = if (hasJsonFilter) {
      val minCountNeeded = offset + safePageSize
      val sampleLimit = minCountNeeded + 1
      val sampleQuery = fr"SELECT COUNT(*) FROM (" ++
        fr"SELECT 1 FROM events" ++ whereClause ++ fr"LIMIT ${sampleLimit}" ++
      fr")"

      sampleQuery.query[Int].unique.transact(xa).map { sampleCount =>
        if (sampleCount > minCountNeeded) {
          (sampleCount, true)
        } else {
          (sampleCount, false)
        }
      }
    } else {
      countQuery.query[Int].unique.transact(xa).map(count => (count, false))
    }

    for {
      countResult <- totalItemsIO
      jsonStrings <- dataQuery.query[String].to[List].transact(xa)
      events <- jsonStrings.traverse { jsonStr =>
        IO.fromEither(parser.parse(jsonStr))
      }
    } yield {
      val (totalItems, approximateCount) = countResult
      val totalPages = Math.max(1, (totalItems + safePageSize - 1) / safePageSize)
      EventsResponse(events, totalPages, totalItems, approximateCount)
    }
  }

  def cleanupExpiredEvents(ttl: java.time.Duration): IO[Unit] = {
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
    val cutoffTime = Instant.now().minus(ttl)
    logger.info(s"Running TTL cleanup: deleting events older than $cutoffTime (TTL: $ttl)") *>
    sql"DELETE FROM events WHERE timestamp < $cutoffTime"
      .update
      .run
      .transact(xa)
      .flatMap { deletedCount =>
        logger.info(s"TTL cleanup completed: deleted $deletedCount events")
      }
  }
}

private[micro] object SqliteStorage {
  // Limit scan depth for columnStats queries to improve performance
  // Only scan most recent N events instead of entire table
  // Tunable: increase for more coverage, decrease for better performance
  private val COLUMN_STATS_SCAN_LIMIT = 1000

  private def databaseExecutionContext: Resource[IO, ExecutionContext] = {
    Resource.make(
      IO {
        val executor = Executors.newFixedThreadPool(5)
        (ExecutionContext.fromExecutor(executor), executor)
      }
    ) { case (_, executor) =>
      IO {
        executor.shutdown()
        // Wait for existing tasks to complete (max 10 seconds)
        // This prevents race conditions during test cleanup
        val _ = executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS)
      }
    }.map(_._1)
  }

  /** Create SQLite storage with file-based database. */
  def file(dbPath: String): Resource[IO, SqliteStorage] = {
    val params = List(
      "journal_mode=WAL",   // Write-Ahead Logging for better concurrency
      "busy_timeout=30000", // Wait up to 30 seconds instead of immediate BUSY error
      "synchronous=NORMAL", // Balance between safety and performance
      "cache_size=-20000",  // 20MB cache (negative = KB)
      "temp_store=MEMORY"   // Use memory for temporary tables
    )
    val url = s"jdbc:sqlite:$dbPath?${params.mkString("&")}"
    create(url)
  }

  private def create(url: String): Resource[IO, SqliteStorage] = {
    for {
      ec <- databaseExecutionContext
      xa <- HikariTransactor.newHikariTransactor[IO](
        driverClassName = "org.sqlite.JDBC",
        url = url,
        user = "", // SQLite doesn't use user/password
        pass = "",
        connectEC = ec
      )
      _ <- Resource.eval(IO {
        xa.kernel match {
          case ds: com.zaxxer.hikari.HikariDataSource =>
            val config = ds.getHikariConfigMXBean
            config.setMaximumPoolSize(5)       // Allow multiple concurrent readers (WAL mode supports this)
            config.setMinimumIdle(1)           // Keep one connection alive
            config.setConnectionTimeout(30000) // 30 second connection timeout
            config.setIdleTimeout(600000)      // 10 minute idle timeout
          case _ =>
            ()
        }
      }.handleErrorWith { err =>
        IO(System.err.println(s"Warning: Failed to configure HikariCP pool: ${err.getMessage}"))
      })
      _ <- Resource.eval(initialize(xa))
    } yield new SqliteStorage(xa)
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
        event_name TEXT
      )
    """.update.run.void *>
      // Single-column indexes
      sql"CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_app_id ON events(app_id)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_event_name ON events(event_name)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_failed ON events(failed)".update.run.void *>
      // Composite indexes for common filtered queries (dramatically improves performance with 10k+ rows)
      sql"CREATE INDEX IF NOT EXISTS idx_events_failed_timestamp ON events(failed, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_app_id_timestamp ON events(app_id, timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_event_name_timestamp ON events(event_name, timestamp)".update.run.void
  }

  private def createColumnsTable: ConnectionIO[Unit] = {
    sql"""
      CREATE TABLE IF NOT EXISTS columns (
        name TEXT PRIMARY KEY
      )
    """.update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_columns_name ON columns(name)".update.run.void
  }

  implicit val instantEpochMeta: Meta[Instant] =
    Meta[Timestamp].timap(_.toInstant)(Timestamp.from)

  implicit val timelinePointRead: Read[TimelinePoint] =
    Read[(Instant, Int, Int)].map { case (timestamp, validEvents, failedEvents) =>
      TimelinePoint(validEvents, failedEvents, timestamp)
    }
}
