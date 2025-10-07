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

final class Routing(igluResolver: Resolver[IO], validationCache: ValidationCache)
                   (implicit lookup: RegistryLookup[IO]) extends Http4sDsl[IO] {

  val disabled: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case _ -> "micro" /: _ => NotFound()
  }

  val enabled: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case request@method -> "micro" /: path =>
      (method, path.segments.headOption.map(_.encoded)) match {
        case (GET, Some("events")) =>
          Ok(validationCache.getGoodAndIncomplete.map(_.event.toJson(lossy = true)))
        case (POST | GET, Some("all")) =>
          Ok(validationCache.getSummary())
        case (POST | GET, Some("reset")) =>
          validationCache.reset()
          Ok(validationCache.getSummary())
        case (GET, Some("good")) =>
          Ok(validationCache.filterGood(FiltersGood(None, None, None, None)))
        case (POST, Some("good")) =>
          request.as[FiltersGood].flatMap { filters =>
            Ok(validationCache.filterGood(filters).asJson)
          }
        case (GET, Some("bad")) =>
          Ok(validationCache.filterBad(FiltersBad(None, None, None)))
        case (POST, Some("bad")) =>
          request.as[FiltersBad].flatMap { filters =>
            Ok(validationCache.filterBad(filters))
          }
        case (GET, Some("iglu")) =>
          path match {
            case Path.empty / "iglu" / vendor / name / "jsonschema" / versionVar =>
              lookupSchema(vendor, name, versionVar)
            case _ =>
              NotFound("Schema lookup should be in format iglu/{vendor}/{schemaName}/jsonschema/{model}-{revision}-{addition}")
          }
        case (GET, Some("ui")) =>
          handleUIPath(path)
        case _ =>
          NotFound("Path for micro has to be one of: /events /all /good /bad /reset /iglu")
      }
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

object Routing {
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