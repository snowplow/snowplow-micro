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

import cats.data.{EitherT, Kleisli, OptionT}
import cats.effect.IO
import com.snowplowanalytics.snowplow.micro.Configuration.AuthConfig
import org.http4s.{AuthedRoutes, Headers, MediaType, Method, Request, Status, Uri}
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.{Accept, Authorization}
import org.http4s.server.AuthMiddleware
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration._

object Auth extends Http4sDsl[IO] {
  implicit private def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val clientResource = EmberClientBuilder.default[IO]
    .withTimeout(10.seconds)
    .withIdleTimeInPool(30.seconds)
    .build

  private def authorize(authHeader: Authorization, authConfig: AuthConfig, client: Client[IO]): IO[Either[String, Unit]] = {
    val uri = Uri.unsafeFromString(s"https://${authConfig.apiDomain}/api/msc/internal/authz/query/v1/${authConfig.organizationId}/minis/list")
    val headers = Headers(authHeader, Accept(MediaType.application.json))
    val request = Request[IO](Method.POST, uri)
      .withHeaders(headers)
      .withEntity("{}")

    client.run(request).use { response =>
      if (response.status == Status.Ok) {
        response.bodyText.compile.string.map { body =>
          if (body.trim == "true") Right(())
          else Left("Authorization denied")
        }
      } else {
        IO.pure(Left(s"Authorization API returned ${response.status}"))
      }
    }.handleError { error =>
      Left(s"Authorization API error: ${error.getMessage}")
    }
  }

  def authMiddleware(authConfig: AuthConfig): AuthMiddleware[IO, Unit] = {
    val authUser: Kleisli[IO, Request[IO], Either[String, Unit]] = Kleisli({ request =>
      clientResource.use { client =>
        (for {
          authHeader <- EitherT.fromOption[IO](request.headers.get[Authorization], "Missing Authorization header")
          // We are proxying the token to Console API without reading or using it in any way.
          // As such, there is no point in running the same validation the Console would do.
          // However, if in the future we need to access the token in Micro, it must be validated:
          //   - use https://github.com/auth0/jwks-rsa-java to automatically fetch the public keys from `authConfig.domain`
          //   - parse the token manually to extract the `kid` and pick the correct key
          //   - use https://github.com/jwt-scala/jwt-scala to check signature, issuer, audience and time validity
          _ <- EitherT(authorize(authHeader, authConfig, client))
        } yield ()).leftSemiflatMap { error =>
          logger.warn(s"Authentication failed: $error").as("Authentication denied")
        }.value
      }
    })

    val onFailure: AuthedRoutes[String, IO] = Kleisli(_ => OptionT.liftF(Forbidden()))

    AuthMiddleware(authUser, onFailure)
  }
}
