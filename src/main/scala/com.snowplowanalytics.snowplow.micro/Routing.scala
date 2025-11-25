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

import cats.data.OptionT
import cats.effect.IO
import cats.implicits.toSemigroupKOps
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.micro.Configuration.AuthConfig
import com.snowplowanalytics.snowplow.micro.Routing._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.apache.http.NameValuePair
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.server.middleware.GZip
import org.http4s.{AuthedRoutes, HttpRoutes, Response, StaticFile}
import org.joda.time.DateTime

import java.time.Instant

sealed trait MicroRoutes[S <: EventStorage] extends Http4sDsl[IO] {
  protected def igluResolver: Resolver[IO]
  protected def storage: S
  protected def authConfig: Option[AuthConfig]
  protected implicit def lookup: RegistryLookup[IO]

  /** Common routes for iglu, ui, and reset endpoints */
  val commonRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case (POST | GET) -> Root / "micro" / "reset" =>
      storage.reset().flatMap(_ => Ok("Reset completed"))
    case request @ POST -> Root / "micro" / "events" =>
      request.as[EventsRequest].flatMap { req =>
        storage.getFilteredEvents(req).flatMap(response => Ok(response))
      }
    case GET -> Root / "micro" / "columns" =>
      storage.getColumns.flatMap(columns => Ok(columns))
    case request @ POST -> Root / "micro" / "columnStats" =>
      request.as[ColumnStatsRequest].flatMap { req =>
        storage.getColumnStats(req.columns).flatMap(stats => Ok(stats))
      }
    case GET -> Root / "micro" / "timeline" =>
      storage.getTimeline.flatMap(timeline => Ok(timeline))
    case GET -> Root / "micro" / "iglu" / vendor / name / "jsonschema" / versionVar =>
      lookupSchema(vendor, name, versionVar)
  }

  private val commonPublicRoutes = HttpRoutes.of[IO] {
    case GET -> "micro" /: path if path.startsWithString("ui") =>
      handleUIPath(path)
    case GET -> Root / "micro" / "auth-config" =>
      authConfig match {
        case Some(config) =>
          Ok(AuthConfigResponse(config.domain, config.audience, config.clientId).asJson)
        case None =>
          Ok(AuthConfigResponse("", "", "", enabled = false).asJson)
      }
  }

  protected def commonEndpoints = List(
    "/micro/events",
    "/micro/reset",
    "/micro/columns",
    "/micro/columnStats",
    "/micro/timeline",
    "/micro/iglu",
    "/micro/ui"
  )

  protected def addAuthMiddleware(routes: HttpRoutes[IO]): HttpRoutes[IO] = {
    val wrapped = authConfig match {
      case Some(auth) =>
        val authedRoutes = AuthedRoutes[Unit, IO](req => routes(req.req))
        val middleware = Auth.authMiddleware(auth)(authedRoutes)
        HttpRoutes[IO] { req =>
          // prevent eagerly requiring auth for everything
          // we need to bypass auth and fall through into collector routes that will come later
          if (req.pathInfo.startsWith(Root / "micro")) middleware(req) else OptionT.none
        }
      case None => routes
    }
    // public routes must be accessible from the frontend without auth
    commonPublicRoutes <+> wrapped
  }

  private def handleUIPath(path: Path): IO[Response[IO]] = {
    path match {
      case Path.empty / "ui" | Path.empty / "ui" / "/" =>
        resource("ui/index.html")
      case other =>
        resource(other.renderString)
    }
  }

  private def resource(path: String): IO[Response[IO]] = {
    StaticFile.fromResource[IO](path)
      .getOrElseF(NotFound())
  }

  private def lookupSchema(vendor: String, name: String, versionVar: String): IO[Response[IO]] = {
    SchemaVer.parseFull(versionVar) match {
      case Right(version) =>
        val key = SchemaKey(vendor, name, "jsonschema", version)
        igluResolver.lookupSchema(key).flatMap {
          case Right(json) => Ok(json)
          case Left(error) => NotFound(error)
        }
      case Left(_) => NotFound("Schema lookup should be in format iglu/{vendor}/{schemaName}/jsonschema/{model}-{revision}-{addition}")
    }
  }
}

final class InMemoryRoutes(protected val igluResolver: Resolver[IO],
                           protected val storage: InMemoryStorage,
                           protected val authConfig: Option[AuthConfig])
                          (implicit protected val lookup: RegistryLookup[IO]) extends MicroRoutes[InMemoryStorage] {
  private val inMemoryRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case (POST | GET) -> Root / "micro" / "all" =>
      Ok(storage.getSummary())
    case GET -> Root / "micro" / "good" =>
      Ok(storage.filterGood(FiltersGood(None, None, None, None)))
    case request @ POST -> Root / "micro" / "good" =>
      request.as[FiltersGood].flatMap { filters =>
        Ok(storage.filterGood(filters).asJson)
      }
    case GET -> Root / "micro" / "bad" =>
      Ok(storage.filterBad(FiltersBad(None, None, None)))
    case request @ POST -> Root / "micro" / "bad" =>
      request.as[FiltersBad].flatMap { filters =>
        Ok(storage.filterBad(filters))
      }
    case _ -> "micro" /: _ =>
      NotFound(s"Supported endpoints: /micro/all, /micro/good, /micro/bad, ${commonEndpoints.mkString(", ")}")
  }

  val routes: HttpRoutes[IO] = GZip(addAuthMiddleware(commonRoutes <+> inMemoryRoutes))
}

final class SqliteRoutes(protected val igluResolver: Resolver[IO],
                         protected val storage: SqliteStorage,
                         protected val authConfig: Option[AuthConfig])
                        (implicit protected val lookup: RegistryLookup[IO]) extends MicroRoutes[SqliteStorage] {
  private val sqliteRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case _ -> "micro" /: _ =>
      NotFound(s"Supported endpoints: ${commonEndpoints.mkString(", ")}")
  }

  val routes: HttpRoutes[IO] = GZip(addAuthMiddleware(commonRoutes <+> sqliteRoutes))
}

object NoRoutes extends Http4sDsl[IO] {
  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case _ -> "micro" /: _ => NotFound()
  }
}

object Routing {
  def create(config: Configuration.MicroConfig, storage: EventStorage, auth: Option[AuthConfig], lookup: RegistryLookup[IO]): HttpRoutes[IO] = {
    storage match {
      case NoStorage => NoRoutes.routes
      case sqliteStorage: SqliteStorage =>
        new SqliteRoutes(config.iglu.resolver, sqliteStorage, auth)(lookup).routes
      case inMemoryStorage: InMemoryStorage =>
        new InMemoryRoutes(config.iglu.resolver, inMemoryStorage, auth)(lookup).routes
    }
  }

  case class AuthConfigResponse(domain: String, audience: String, clientId: String, enabled: Boolean = true)

  implicit val dateTimeEncoder: Encoder[DateTime] =
    Encoder[String].contramap(_.toString)

  implicit val instantEncoder: Encoder[Instant] =
    Encoder[Long].contramap(_.toEpochMilli)

  implicit val instantDecoder: Decoder[Instant] =
    Decoder[Long].map(Instant.ofEpochMilli)

  implicit val nameValuePairEncoder: Encoder[NameValuePair] =
    Encoder[String].contramap(kv => s"${kv.getName}=${kv.getValue}")

  implicit val vs: Encoder[ValidationSummary] = deriveEncoder
  implicit val ge: Encoder[GoodEvent] = deriveEncoder
  implicit val rwe: Encoder[RawEvent] = deriveEncoder
  implicit val cp: Encoder[CollectorPayload] = deriveEncoder
  implicit val cpa: Encoder[CollectorPayload.Api] = deriveEncoder
  implicit val cps: Encoder[CollectorPayload.Source] = deriveEncoder
  implicit val cpc: Encoder[CollectorPayload.Context] = deriveEncoder
  implicit val e: Encoder[Event] = deriveEncoder
  implicit val be: Encoder[BadEvent] = deriveEncoder
  implicit val re: Encoder[ResolutionError] = deriveEncoder
  implicit val tp: Encoder[TimelinePoint] = deriveEncoder
  implicit val td: Encoder[TimelineData] = deriveEncoder
  implicit val cs: Encoder[ColumnStats] = deriveEncoder
  implicit val csresp: Encoder[ColumnStatsResponse] = deriveEncoder
  implicit val ef: Encoder[EventsFilter] = deriveEncoder
  implicit val tr: Encoder[TimeRange] = deriveEncoder
  implicit val es: Encoder[EventsSorting] = deriveEncoder
  implicit val er: Encoder[EventsResponse] = deriveEncoder
  implicit val acr: Encoder[AuthConfigResponse] = deriveEncoder

  implicit val fg: Decoder[FiltersGood] = deriveDecoder
  implicit val fb: Decoder[FiltersBad] = deriveDecoder
  implicit val csr: Decoder[ColumnStatsRequest] = deriveDecoder
  implicit val efd: Decoder[EventsFilter] = deriveDecoder
  implicit val trd: Decoder[TimeRange] = deriveDecoder
  implicit val esd: Decoder[EventsSorting] = deriveDecoder
  implicit val erd: Decoder[EventsRequest] = deriveDecoder
}
