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

    // Only fetch stats for indexed columns (SQLite mode doesn't support JSON column filtering)
    val indexedColumns = columns
      .filter(SqliteStorage.INDEXED_COLUMNS.contains)
      .filterNot(EventStorage.isComplexColumn)
      .filterNot(EventStorage.isTimestampColumn)

    indexedColumns.parTraverse { column =>
      val query = fr"SELECT DISTINCT value FROM (" ++
        fr"SELECT" ++ Fragment.const(column) ++ fr"as value FROM events" ++
        fr"ORDER BY timestamp DESC LIMIT ${COLUMN_STATS_SCAN_LIMIT}" ++
      fr") WHERE value IS NOT NULL LIMIT 20"

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

    // Only filter on indexed columns (SQLite mode doesn't support JSON column filtering)
    val columnFilters = request.filters
      .filter(f => SqliteStorage.INDEXED_COLUMNS.contains(f.column))
      .filterNot(f => EventStorage.isComplexColumn(f.column))
      .filter(_.value.nonEmpty) // Skip empty filter values
      .map { filter =>
        fr"AND" ++ Fragment.const(filter.column) ++ fr"=" ++ fr"${filter.value}" ++ fr"COLLATE NOCASE"
      }

    val allConditions = whereConditions ++ columnFilters
    val whereClause = allConditions.foldLeft(baseConditions)(_ ++ _)

    // Only allow sorting on indexed columns (SQLite mode doesn't support JSON column sorting)
    val orderByClause = request.sorting.filterNot(s =>
      EventStorage.isComplexColumn(s.column)
    ).filter(s => s.column == "collector_tstamp" || SqliteStorage.INDEXED_COLUMNS.contains(s.column)
    ).fold(fr"ORDER BY timestamp DESC") { sorting =>
        val columnExpr = if (sorting.column == "collector_tstamp") {
          Fragment.const("timestamp")
        } else {
          Fragment.const(sorting.column)
        }
        val direction = if (sorting.desc) "DESC" else "ASC"
        fr"ORDER BY" ++ columnExpr ++ Fragment.const(s" $direction")
      }

    // prevent OOM when an unreasonably large page size is supplied
    // the UI only uses 50 currently
    val safePageSize = Math.max(Math.min(request.pageSize, 100), 1)
    val offset = (request.page - 1) * safePageSize

    val countQuery = fr"SELECT COUNT(*) FROM events" ++ whereClause
    val dataQuery = fr"SELECT event_json FROM events" ++
      whereClause ++ orderByClause ++
      fr"LIMIT ${safePageSize} OFFSET ${offset}"

    for {
      totalItems <- countQuery.query[Int].unique.transact(xa)
      jsonStrings <- dataQuery.query[String].to[List].transact(xa)
      events <- jsonStrings.traverse { jsonStr =>
        IO.fromEither(parser.parse(jsonStr))
      }
    } yield {
      val totalPages = Math.max(1, (totalItems + safePageSize - 1) / safePageSize)
      EventsResponse(events, totalPages, totalItems)
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
  // Columns that have dedicated table columns (rather than JSON extraction)
  private val INDEXED_COLUMNS = Set("event_id", "app_id", "event_name", "platform", "name_tracker", "domain_userid", "v_tracker")

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
          case other =>
            throw new IllegalStateException(s"Expected HikariDataSource but got ${other.getClass.getName}")
        }
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
        event_name TEXT,
        platform TEXT,
        name_tracker TEXT,
        domain_userid TEXT,
        v_tracker TEXT
      )
    """.update.run.void *>
      // Single-column indexes
      sql"CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_failed ON events(failed)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_app_id ON events(app_id)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_event_name ON events(event_name)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_platform ON events(platform)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_name_tracker ON events(name_tracker)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_domain_userid ON events(domain_userid)".update.run.void *>
      sql"CREATE INDEX IF NOT EXISTS idx_events_v_tracker ON events(v_tracker)".update.run.void *>
      // Composite indexes for filtered queries (dramatically improves performance with 10k+ rows)
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

  implicit val instantEpochMeta: Meta[Instant] =
    Meta[Timestamp].timap(_.toInstant)(Timestamp.from)

  implicit val timelinePointRead: Read[TimelinePoint] =
    Read[(Instant, Int, Int)].map { case (timestamp, validEvents, failedEvents) =>
      TimelinePoint(validEvents, failedEvents, timestamp)
    }
}
