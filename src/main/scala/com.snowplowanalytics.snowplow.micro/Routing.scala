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
import cats.implicits.toSemigroupKOps
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.micro.Routing._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.apache.http.NameValuePair
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response, StaticFile}
import org.joda.time.DateTime

sealed trait BaseRouting[S <: EventStorage] extends Http4sDsl[IO] {
  protected def igluResolver: Resolver[IO]
  protected def storage: S
  protected implicit def lookup: RegistryLookup[IO]

  /** Common routes for iglu, ui, and reset endpoints */
  def commonRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case (POST | GET) -> Root / "micro" / "reset" =>
      storage.reset().flatMap(_ => Ok("Reset completed"))
    case GET -> Root / "micro" / "iglu" / vendor / name / "jsonschema" / versionVar =>
      lookupSchema(vendor, name, versionVar)
    case GET -> "micro" /: path if path.startsWithString("ui") =>
      handleUIPath(path)
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

final class InMemoryRouting(protected val igluResolver: Resolver[IO], protected val storage: InMemoryStorage)
                           (implicit protected val lookup: RegistryLookup[IO]) extends BaseRouting[InMemoryStorage] {
  private val inMemoryRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "micro" / "events" =>
      Ok(storage.getGoodAndIncomplete.map(_.event.toJson(lossy = true)))
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
      NotFound("Supported endpoints: /micro/events, /micro/all, /micro/good, /micro/bad, /micro/reset, /micro/iglu, /micro/ui")
  }

  val routes: HttpRoutes[IO] = commonRoutes <+> inMemoryRoutes
}

final class SqliteRouting(protected val igluResolver: Resolver[IO],
                          protected val storage: SqliteStorage)
                         (implicit protected val lookup: RegistryLookup[IO]) extends BaseRouting[SqliteStorage] {
  private val sqliteRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "micro" / "events" =>
      storage.getEvents.flatMap(events => Ok(events))
    case _ -> "micro" /: _ =>
      NotFound("Supported endpoints: /micro/events, /micro/reset, /micro/iglu, /micro/ui")
  }

  val routes: HttpRoutes[IO] = commonRoutes <+> sqliteRoutes
}

object NoRoutes extends Http4sDsl[IO] {
  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case _ -> "micro" /: _ => NotFound()
  }
}

object Routing {
  def create(config: Configuration.MicroConfig, storage: EventStorage, lookup: RegistryLookup[IO]): HttpRoutes[IO] = {
    storage match {
      case NoStorage => NoRoutes.routes
      case sqliteStorage: SqliteStorage =>
        new SqliteRouting(config.iglu.resolver, sqliteStorage)(lookup).routes
      case inMemoryStorage: InMemoryStorage =>
        new InMemoryRouting(config.iglu.resolver, inMemoryStorage)(lookup).routes
    }
  }

  implicit val dateTimeEncoder: Encoder[DateTime] =
    Encoder[String].contramap(_.toString)

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

  implicit val fg: Decoder[FiltersGood] = deriveDecoder
  implicit val fb: Decoder[FiltersBad] = deriveDecoder
}