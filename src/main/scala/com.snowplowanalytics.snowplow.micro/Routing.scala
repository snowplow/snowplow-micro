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

import cats.data.{OptionT}
import cats.effect.IO
import com.snowplowanalytics.iglu.client.ClientError.ResolutionError
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.enrich.common.adapters.RawEvent
import com.snowplowanalytics.snowplow.enrich.common.loaders.CollectorPayload
import com.snowplowanalytics.snowplow.micro.Routing._
import com.snowplowanalytics.snowplow.micro.Configuration.AuthConfig
import cats.implicits._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Encoder}
import org.apache.http.NameValuePair
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.{HttpRoutes, Response, StaticFile}
import org.http4s.AuthedRoutes
import org.joda.time.DateTime

final class Routing(igluResolver: Resolver[IO], validationCache: ValidationCache, authConfig: Option[AuthConfig])
                   (implicit lookup: RegistryLookup[IO]) extends Http4sDsl[IO] {

  private val baseRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
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
        case _ =>
          NotFound("Path for micro has to be one of: /events /all /good /bad /reset /iglu")
      }
  }

  private val authConfigRoute: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> Root / "micro" / "auth-config" =>
      authConfig match {
        case Some(config) =>
          Ok(AuthConfigResponse(config.domain, config.audience, config.clientId).asJson)
        case None =>
          Ok(AuthConfigResponse("", "", "", enabled = false).asJson)
      }
  }

  private val uiRoutes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case GET -> "micro" /: path if path.segments.headOption.map(_.encoded).contains("ui") =>
      path match {
        case Path.empty / "ui" | Path.empty / "ui" / "/" =>
          resource("ui/index.html")
        case other =>
          resource(other.renderString)
      }
  }

  val disabled: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case _ -> "micro" /: _ => NotFound()
  }

  val enabled: HttpRoutes[IO] = {
    val apiRoutes = authConfig match {
      case Some(config) =>
        val authedRoutes = AuthedRoutes[Unit, IO](req => baseRoutes(req.req))
        val middleware = Auth.authMiddleware(config)(authedRoutes)
        HttpRoutes[IO] { req =>
          // prevent eagerly requiring auth for everything
          // we need to bypass auth and fall through into collector routes that will come later
          if (req.pathInfo.startsWith(Root / "micro")) middleware(req) else OptionT.none
        }
      case None =>
        baseRoutes
    }

    uiRoutes <+> authConfigRoute <+> apiRoutes
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
  case class AuthConfigResponse(domain: String, audience: String, clientId: String, enabled: Boolean = true)

  implicit val dateTimeEncoder: Encoder[DateTime] =
    Encoder[String].contramap(_.toString)

  implicit val nameValuePairEncoder: Encoder[NameValuePair] =
    Encoder[String].contramap(kv => s"${kv.getName}=${kv.getValue}")

  implicit val acr: Encoder[AuthConfigResponse] = deriveEncoder
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