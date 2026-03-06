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
import cats.effect.std.AtomicCell
import cats.implicits._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.io.{FileOutputStream, OutputStream}
import java.net.URI
import java.nio.ByteBuffer
import scala.concurrent.duration._

object AssetRefresher {

  implicit private def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  case class AssetState(uri: URI, filePath: String, etag: Option[String])
  private case class UpdatedAsset(filePath: String, content: ByteBuffer)

  def makeClients(configs: List[EnrichmentConf]): Resource[IO, Map[BlobClient, BlobClientImpl]] = {
    val assets = configs.flatMap(_.filesToCache)
    val blobClients = assets.map { case (uri, _) => BlobUtils.blobClientFor(uri) }.distinct
    blobClients.traverse { bc =>
      bc.mk.map(impl => bc -> impl)
    }.map(_.toMap)
  }

  def initialDownload(
    configs: List[EnrichmentConf],
    clients: Map[BlobClient, BlobClientImpl]
  ): IO[AtomicCell[IO, List[AssetState]]] = {
    val assets = configs.flatMap(_.filesToCache)
    for {
      results <- downloadAllAssets(assets.map { case (uri, path) => AssetState(uri, path, None) }, clients, tolerateErrors = false)
      _ <- results.flatMap(_._2).traverse_(asset => writeToFile(asset.content, asset.filePath))
      atomicCell <- AtomicCell[IO].of(results.map(_._1))
    } yield atomicCell
  }

  def updateStream(
    period: FiniteDuration,
    atomicCell: AtomicCell[IO, List[AssetState]],
    clients: Map[BlobClient, BlobClientImpl],
    registryColdswap: Coldswap[IO, EnrichmentRegistry[IO]]
  ): Stream[IO, Nothing] = {
    Stream.fixedDelay[IO](period).evalMap { _ =>
      refreshAssets(atomicCell, clients, registryColdswap)
        .handleErrorWith { error =>
          logger.warn(s"Asset refresh failed, keeping existing registry: ${error.getMessage}")
        }
    }.drain
  }

  private def refreshAssets(
    atomicCell: AtomicCell[IO, List[AssetState]],
    clients: Map[BlobClient, BlobClientImpl],
    registryColdswap: Coldswap[IO, EnrichmentRegistry[IO]]
  ): IO[Unit] = {
    atomicCell.evalUpdate { currentState =>
      downloadAllAssets(currentState, clients, tolerateErrors = true).flatMap { results =>
        val updatedAssets = results.flatMap(_._2)
        val newStates = results.map(_._1)
        if (updatedAssets.nonEmpty)
          for {
            _ <- logger.info("Asset changes detected, rebuilding enrichment registry")
            _ <- registryColdswap.closed.surround {
              updatedAssets.traverse_(asset => writeToFile(asset.content, asset.filePath))
            }
          } yield newStates
        else
          logger.debug("No asset changes detected").as(currentState)
      }
    }
  }

  private def downloadAllAssets(
    assets: List[AssetState],
    clients: Map[BlobClient, BlobClientImpl],
    tolerateErrors: Boolean
  ): IO[List[(AssetState, Option[UpdatedAsset])]] = {
    assets
      .groupBy(asset => BlobUtils.blobClientFor(asset.uri))
      .toList
      .flatTraverse { case (blobClient, groupedAssets) =>
        val client = clients(blobClient)
        groupedAssets.traverse { asset =>
          val download = downloadSingleAsset(client, asset)
          if (tolerateErrors)
            download.handleErrorWith { error =>
              logger.warn(s"Failed to check asset ${asset.uri}: ${error.getMessage}")
                .as((asset, None))
            }
          else
            download
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

  private def writeToFile(content: ByteBuffer, filePath: String): IO[Unit] =
    Stream
      .chunk(fs2.Chunk.byteBuffer(content))
      .through(fs2.io.writeOutputStream(IO.blocking[OutputStream](new FileOutputStream(filePath))))
      .compile
      .drain
}
