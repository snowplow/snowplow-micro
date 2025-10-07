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
import cats.effect.kernel.Resource
import cats.effect.testing.specs2.CatsEffect
import io.circe.Json
import org.http4s.Method.GET
import org.http4s.Request
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.CirceEntityCodec.circeEntityDecoder
import org.http4s.client.Client
import org.http4s.implicits.http4sLiteralsSyntax
import org.specs2.mutable.Specification
import org.http4s.client.middleware.{Retry, RetryPolicy}

import scala.concurrent.duration.{Duration, DurationInt}

class MicroApiSpec extends Specification with CatsEffect {
  sequential

  override protected val Timeout: Duration = 1.minute 


  "Micro should accept good data" in {
    setup().use { client =>
      for {
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=lol")).use_
        _ <- IO.sleep(1.seconds)
        all <- client.run(Request(GET, uri"http://localhost:9090/micro/all")).use(_.as[Json])
        good <- client.run(Request(GET, uri"http://localhost:9090/micro/good")).use(_.as[Json])
      } yield {

        all.noSpaces must beEqualTo("""{"total":1,"good":1,"bad":0}""")
        good.isArray must beTrue
        good.asArray.map(_.length).get must beEqualTo(1)

        val firstEvent = good.hcursor.downN(0)
        firstEvent.downField("eventType").as[String] must beRight("page_ping")
        firstEvent.downField("schema").as[String] must beRight("iglu:com.snowplowanalytics.snowplow/page_ping/jsonschema/1-0-0")
        firstEvent.downField("event").downField("platform").as[String] must beRight("web")
        firstEvent.downField("event").downField("v_tracker").as[String] must beRight("lol")
      }
    }
  }

  "Micro should handle bad data" in {
    setup().use { client =>
      for {
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=lol&eid=invalidEventId")).use_
        _ <- IO.sleep(1.seconds)
        all <- client.run(Request(GET, uri"http://localhost:9090/micro/all")).use(_.as[Json])
        bad <- client.run(Request(GET, uri"http://localhost:9090/micro/bad")).use(_.as[Json])
      } yield {

        bad.isArray must beTrue
        bad.asArray.map(_.length).get must beEqualTo(1)
        all.noSpaces must beEqualTo("""{"total":1,"good":0,"bad":1}""")
      }
    }
  }

  "Micro should reset stored data" in {
    setup().use { client =>
      for {
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=lol")).use_
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=lol&eid=invalidEventId")).use_
        _ <- IO.sleep(1.seconds)
        beforeReset <- client.run(Request(GET, uri"http://localhost:9090/micro/all")).use(_.as[Json])
        afterReset <- client.run(Request(GET, uri"http://localhost:9090/micro/reset")).use(_.as[Json])
      } yield {

        beforeReset.noSpaces must beEqualTo("""{"total":2,"good":1,"bad":1}""")
        afterReset.noSpaces must beEqualTo("""{"total":0,"good":0,"bad":0}""")
      }
    }
  }

  "Micro with --max-events 0 should disable micro endpoints" in {
    setupWithArgs(List("--max-events", "0")).use { client =>
      for {
        allStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/all")).use(r => IO.pure(r.status))
        goodStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/good")).use(r => IO.pure(r.status))
        badStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/bad")).use(r => IO.pure(r.status))
        eventsStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/events")).use(r => IO.pure(r.status))
        uiStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/ui")).use(r => IO.pure(r.status))
        resetStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/reset")).use(r => IO.pure(r.status))
        igluStatus <- client.run(Request(GET, uri"http://localhost:9090/micro/iglu")).use(r => IO.pure(r.status))
        // Health endpoint should still work
        healthStatus <- client.run(Request(GET, uri"http://localhost:9090/health")).use(r => IO.pure(r.status))
      } yield {
        allStatus.code must_== 404
        goodStatus.code must_== 404
        badStatus.code must_== 404
        eventsStatus.code must_== 404
        uiStatus.code must_== 404
        resetStatus.code must_== 404
        igluStatus.code must_== 404
        healthStatus.code must_== 200
      }
    }
  }

  "Micro with --max-events should limit cache size" in {
    setupWithArgs(List("--max-events", "2")).use { client =>
      for {
        // Send 3 events
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=event1")).use_
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=event2")).use_
        _ <- client.run(Request(GET, uri"http://localhost:9090/i?e=pp&p=web&tv=event3")).use_
        _ <- IO.sleep(1.seconds)
        all <- client.run(Request(GET, uri"http://localhost:9090/micro/all")).use(_.as[Json])
      } yield {
        // Should only keep 2 events due to rolling cache
        all.noSpaces must beEqualTo("""{"total":2,"good":2,"bad":0}""")
      }
    }
  }

  private def setup(): Resource[IO, Client[IO]] = setupWithArgs(List.empty)

  private def setupWithArgs(args: List[String]): Resource[IO, Client[IO]] = {
    for {
      _ <- Main.run(args).background
      client <- buildClient()
      _ <- waitUntilHealthy(client)
    } yield client
  }

  private def buildClient(): Resource[IO, Client[IO]] = {
    val retryPolicy = RetryPolicy[IO](
      RetryPolicy.exponentialBackoff(2.seconds, 5),
      { case (_, result) => RetryPolicy.isErrorOrRetriableStatus(result) }
    )
    BlazeClientBuilder.apply[IO].resource.map { client =>
      Retry[IO](retryPolicy)(client)
    }
  }

  private def waitUntilHealthy(client: Client[IO]): Resource[IO, Unit] = {
    client.run(Request(GET, uri"http://localhost:9090/health"))
      .flatMap { response =>
        if (response.status.code != 200) 
          Resource.raiseError[IO, Unit, Throwable](new RuntimeException("Micro is not healthy"))
        else Resource.unit[IO]
      }
  }
}