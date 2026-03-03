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

import cats.effect.{IO, Ref}
import cats.implicits._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.URI
import java.nio.ByteBuffer
import scala.concurrent.duration._

object AssetRefresher {

  implicit private def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  case class AssetState(uri: URI, filePath: String, etag: Option[String])
  private case class UpdatedAsset(filePath: String, content: ByteBuffer)

  def initialDownload(configs: List[EnrichmentConf]): IO[Ref[IO, List[AssetState]]] = {
    val assets = configs.flatMap(_.filesToCache)
    for {
      results <- downloadAllAssets(assets.map { case (uri, path) => AssetState(uri, path, None) })
      _ <- results.flatMap(_._2).traverse_(asset => writeToFile(asset.content, asset.filePath))
      stateRef <- Ref.of[IO, List[AssetState]](results.map(_._1))
    } yield stateRef
  }

  def updateStream(
    period: FiniteDuration,
    stateRef: Ref[IO, List[AssetState]],
    registryColdswap: Coldswap[IO, EnrichmentRegistry[IO]]
  ): Stream[IO, Nothing] = {
    Stream.fixedDelay[IO](period).evalMap { _ =>
      refreshAssets(stateRef, registryColdswap)
        .handleErrorWith { error =>
          logger.warn(s"Asset refresh failed, keeping existing registry: ${error.getMessage}")
        }
    }.drain
  }

  private def refreshAssets(
    stateRef: Ref[IO, List[AssetState]],
    registryColdswap: Coldswap[IO, EnrichmentRegistry[IO]]
  ): IO[Unit] = {
    for {
      currentState <- stateRef.get
      results <- downloadAllAssets(currentState)
      updatedAssets = results.flatMap(_._2)
      _ <- if (updatedAssets.nonEmpty) {
        val newStates = results.map(_._1)
        for {
          _ <- logger.info("Asset changes detected, rebuilding enrichment registry")
          _ <- registryColdswap.closed.surround {
            updatedAssets.traverse_(asset => writeToFile(asset.content, asset.filePath))
          }
          _ <- stateRef.set(newStates)
        } yield ()
      } else {
        logger.debug("No asset changes detected")
      }
    } yield ()
  }

  private def downloadAllAssets(assets: List[AssetState]): IO[List[(AssetState, Option[UpdatedAsset])]] = {
    assets
      .groupBy(asset => BlobUtils.blobClientFor(asset.uri))
      .toList
      .flatTraverse { case (blobClient, groupedAssets) =>
        blobClient.mk.use { client =>
          groupedAssets.traverse { asset =>
            downloadSingleAsset(client, asset)
              .handleErrorWith { error =>
                logger.warn(s"Failed to check asset ${asset.uri}: ${error.getMessage}")
                  .as((asset, None))
              }
          }
        }
      }
  }

  private def downloadSingleAsset(client: BlobClientImpl, asset: AssetState): IO[(AssetState, Option[UpdatedAsset])] = {
    client.downloadIfNeeded(asset.uri, asset.etag).flatMap {
      case Downloaded(content, newEtag) =>
        logger.info(s"Downloaded asset: ${asset.uri} -> ${asset.filePath}")
          .as((asset.copy(etag = newEtag), Some(UpdatedAsset(asset.filePath, content))))
      case NotModified =>
        IO.pure((asset, None))
    }
  }

  private def writeToFile(content: ByteBuffer, filePath: String): IO[Unit] = {
    Stream.chunk(fs2.Chunk.byteBuffer(content))
      .through(Files[IO].writeAll(Path(filePath)))
      .compile
      .drain
  }
}
