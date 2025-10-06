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

import cats.data.EitherT
import cats.effect.{ExitCode, IO, Resource}
import cats.effect.std.Queue
import cats.implicits._
import com.monovore.decline.Opts
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.collector.core._
import com.snowplowanalytics.snowplow.collector.core.Sinks
import com.snowplowanalytics.snowplow.collector.thrift.CollectorPayload
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.{Enrichment, EnrichmentConf, IpLookupExecutionContext}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery.SqlExecutionContext
import com.snowplowanalytics.snowplow.enrich.common.utils.HttpClient
import com.snowplowanalytics.snowplow.micro.Configuration.MicroConfig
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.client.Client
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.File
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}
import scala.sys.process._

object Run {

  implicit private def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def run(): Opts[IO[ExitCode]] = {
    Configuration.load().map { configuration =>
      handleAppErrors {
        configuration
          .semiflatMap { validMicroConfig =>
            buildEnvironment(validMicroConfig)
              .use(_ => IO.never)
              .as(ExitCode.Success)
          }
      }
    }
  }

  private def buildEnvironment(config: MicroConfig): Resource[IO, Unit] = {
    for {
      sslContext <- Resource.eval(setupSSLContext())
      httpClient <- EmberClientBuilder.default[IO].build
      enrichmentRegistry <- buildEnrichmentRegistry(config.enrichmentsConfig, httpClient)
      badProcessor = Processor(BuildInfo.name, BuildInfo.version)
      lookup = JavaNetRegistryLookup.ioLookupInstance[IO]
      queue <- Resource.eval(Queue.unbounded[IO, CollectorPayload])
      validationCache = new ValidationCache(config.maxEvents)
      sink = new MemorySink(config.iglu.client, lookup, enrichmentRegistry, config.outputFormat, config.destination, badProcessor, config.enrichConfig, httpClient, validationCache)
      sinks = Sinks(sink, sink)
      _ <- Sinks
        .dequeue(config.collector, BuildInfo, queue, sinks)
        .compile
        .drain
        .background
      collectorService = new Service[IO](
        config.collector,
        queue,
        BuildInfo
      )
      collectorRoutes = new Routes[IO](
        config.collector.enableDefaultRedirect,
        config.collector.rootResponse.enabled,
        config.collector.crossDomain.enabled,
        collectorService,
        IO.pure(true)
      )

      microRouting = new Routing(config.iglu.resolver, validationCache)(lookup)
      microRoutes = if (config.maxEvents.contains(0)) microRouting.disabled else microRouting.enabled
      allRoutes = microRoutes <+> collectorRoutes.value <+> collectorRoutes.health
      _ <- MicroHttpServer.build(allRoutes, config, sslContext)
    } yield ()
  }

  private def setupSSLContext(): IO[Option[SSLContext]] = IO {
    sys.env.get(Configuration.EnvironmentVariables.sslCertificatePassword).map { password =>
      // Adapted from https://doc.akka.io/docs/akka-http/current/server-side/server-https-support.html.
      // We could use SSLContext.getDefault instead of all of this, but then we would need to
      // force the user to add arcane -D flags when running Micro, which is not the best experience.
      val keystore = KeyStore.getInstance("PKCS12")
      val certificateFile = getClass.getClassLoader.getResourceAsStream("ssl-certificate.p12")
      keystore.load(certificateFile, password.toCharArray)

      val keyManagerFactory = KeyManagerFactory.getInstance("SunX509")
      keyManagerFactory.init(keystore, password.toCharArray)

      val trustManagerFactory = TrustManagerFactory.getInstance("SunX509")
      trustManagerFactory.init(keystore)

      val context = SSLContext.getInstance("TLS")
      context.init(keyManagerFactory.getKeyManagers, trustManagerFactory.getTrustManagers, new SecureRandom)
      context
    }
  }

  private def buildEnrichmentRegistry(configs: List[EnrichmentConf], httpClient: Client[IO]): Resource[IO, EnrichmentRegistry[IO]] = {
    for {
      _ <- Resource.eval(downloadAssets(configs))
      ipLookupEC <- IpLookupExecutionContext.mk[IO]
      sqlEC <- SqlExecutionContext.mk[IO]
      enrichHttpClient = HttpClient.fromHttp4sClient[IO](httpClient)
      enrichmentRegistry <- Resource.eval(EnrichmentRegistry.build[IO](configs, enrichHttpClient, ipLookupEC, sqlEC, false)
        .leftMap(error => new IllegalArgumentException(s"can't build EnrichmentRegistry: $error"))
        .value.rethrow)
      _ <- Resource.eval {
        val loadedEnrichments = enrichmentRegistry.productIterator.toList.collect {
          case Some(e: Enrichment) => e.getClass.getSimpleName
        }
        if (loadedEnrichments.nonEmpty) {
          logger.info(s"Enabled enrichments: ${loadedEnrichments.mkString(", ")}")
        } else {
          logger.info(s"No enrichments enabled")
        }
      }

    } yield enrichmentRegistry
  }

  private def downloadAssets(configs: List[EnrichmentConf]): IO[Unit] = {
    configs
      .flatMap(_.filesToCache)
      .traverse_ { case (uri, location) =>
        logger.info(s"Downloading $uri...") *> IO(uri.toURL #> new File(location) !!)
      }
  }

  private def handleAppErrors(appOutput: EitherT[IO, String, ExitCode]): IO[ExitCode] = {
    appOutput
      .leftSemiflatMap { error =>
        logger.error(error).as(ExitCode.Error)
      }
      .merge
      .handleErrorWith { exception =>
        logger.error(exception)("Exiting") >>
          prettyLogException(exception).as(ExitCode.Error)
      }
  }

  private def prettyLogException(e: Throwable): IO[Unit] = {

    def logCause(e: Throwable): IO[Unit] =
      Option(e.getCause) match {
        case Some(e) => logger.error(s"caused by: ${e.getMessage}") >> logCause(e)
        case None => IO.unit
      }

    logger.error(e.getMessage) >> logCause(e)
  }


}
