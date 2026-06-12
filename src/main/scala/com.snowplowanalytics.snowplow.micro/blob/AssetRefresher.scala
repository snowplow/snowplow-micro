/*
 * Copyright (c) 2019-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.micro.blob

import java.io.{FileOutputStream, OutputStream}
import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.{FileSystems, Path => NioPath}

import scala.concurrent.duration._
import scala.language.higherKinds
import scala.util.control.NonFatal

import cats.{Applicative, Eq, Foldable}
import cats.implicits._

import cats.effect.{Async, Resource, Sync}
import cats.effect.std.AtomicCell
import cats.effect.implicits._

import retry.{RetryDetails, RetryPolicies, RetryPolicy, retryingOnSomeErrors}

import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.{Chunk, Stream}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf

/**
 * Periodically re-downloads enrichment assets (MaxMind DBs, referer-parser, etc.) and hot-swaps the
 * affected enrichments when their files change.
 *
 * Adapted from Enrich's `AssetRefresher` to keep feature and error-handling parity:
 *   - conditional downloads using ETags (If-None-Match), with an MD5 fallback for sources that don't
 *     provide ETags (e.g. plain HTTP servers);
 *   - retries on transient failures;
 *   - on giving up after retries, the failure is re-raised, which crashes Micro.
 */
class AssetRefresher[F[_]: Async](
  atomicCell: AtomicCell[F, List[AssetRefresher.EnrichmentAssetGroup[F]]]
) {
  import AssetRefresher._

  /**
   * Checks for updated enrichment assets and reloads affected enrichments if any changed.
   *
   * Downloads all assets in parallel per group. For each group, if any asset in the group has
   * changed, the group's `onUpdate` callback is invoked to hot-swap that enrichment. Any failure —
   * whether downloading assets or invoking `onUpdate` — is logged and then re-raised, crashing the
   * application. Assets that haven't changed return 304 Not Modified without downloading content.
   */
  def refresh: F[Unit] =
    atomicCell.evalUpdate { groups =>
      groups.traverse { group =>
        group.assets
          .parTraverse(downloadAsset(group.name, _))
          .flatMap { newStatuses =>
            val hadUpdate = group.assets.zip(newStatuses).exists {
              case (asset, newStatus) => asset.status =!= newStatus
            }
            if (hadUpdate) {
              val newAssets = group.assets.zip(newStatuses).map {
                case (asset, newStatus) => asset.copy(status = newStatus)
              }
              Logger[F].info(s"Assets changed for enrichment ${group.name}, invoking hot-swap") >>
                group.onUpdate
                  .onError {
                    case err =>
                      Logger[F].error(err)(s"Failed to hot-swap enrichment ${group.name} after asset update")
                  }
                  .as(group.copy(assets = newAssets))
            } else
              group.pure[F]
          }
      }
    }
}

object AssetRefresher {

  /**
   * Tracks the download status of an enrichment asset.
   * Used to determine if the asset needs to be re-downloaded.
   */
  private[micro] sealed trait AssetStatus

  /** Asset has not been downloaded yet. Initial state before first download attempt. */
  private[micro] case object NotDownloaded extends AssetStatus

  /** Asset has been successfully downloaded. Contains a value (etag or MD5) for detecting future changes. */
  private[micro] sealed trait AssetSuccessfulStatus extends AssetStatus

  /**
   * Asset downloaded with an etag from the blob storage service.
   * The etag enables efficient conditional requests (If-None-Match) on subsequent downloads.
   * Most blob storage backends (S3, GCS, Azure) provide etags.
   */
  private[micro] case class Etag(value: String) extends AssetSuccessfulStatus

  /**
   * Asset downloaded without an etag.
   * MD5 hash computed locally to detect changes on subsequent downloads.
   * Used when the blob storage backend does not provide etags (e.g., plain HTTP servers).
   * Less efficient than etag-based detection since the entire file must be downloaded to compare.
   */
  private[micro] case class Md5(value: String) extends AssetSuccessfulStatus

  private implicit def assetStatusEq: Eq[AssetStatus] = Eq.fromUniversalEquals

  private[micro] case class Asset[F[_]](
    client: BlobClient[F],
    uri: URI,
    localPath: NioPath,
    status: AssetStatus
  )

  /**
   * A group of assets belonging to a single enrichment, plus a callback to invoke when any of the
   * assets in the group changes.
   */
  private[micro] case class EnrichmentAssetGroup[F[_]](
    name: String,
    assets: List[Asset[F]],
    onUpdate: F[Unit]
  )

  /**
   * The result of the initial asset download phase.
   * Carries the downloaded asset groups (with up-to-date status) and the blob clients so they can be
   * reused for subsequent refreshes.
   */
  private[micro] case class DownloadedAssets[F[_]](
    groups: List[DownloadedGroup[F]]
  )

  /**
   * A group with its assets already downloaded and status updated, but without an `onUpdate`
   * callback yet. The callback is wired in `ManagedEnrichmentRegistry.build`.
   */
  private[micro] case class DownloadedGroup[F[_]](
    conf: EnrichmentConf.WithAssets,
    assets: List[Asset[F]]
  )

  private implicit def unsafeLogger[F[_]: Sync]: Logger[F] =
    Slf4jLogger.getLogger[F]

  def initialDownload[F[_]: Async](
    enrichmentConfs: List[EnrichmentConf],
    blobClients: List[BlobClientFactory[F]]
  ): Resource[F, DownloadedAssets[F]] = {

    val confsWithAssets = enrichmentConfs.collect { case c: EnrichmentConf.WithAssets => c }

    type FileGroup = List[(BlobClientFactory[F], URI, NioPath)]

    // For each conf, build the list of (uri, localPath) pairs, resolved against a blob client.
    val groupsF: F[List[(EnrichmentConf.WithAssets, FileGroup)]] =
      confsWithAssets.traverse { conf =>
        Foldable[List]
          .foldM[F, (URI, String), FileGroup](conf.filesToCache, List.empty) {
            case (acc, (uri, localPath)) =>
              blobClients.find(_.canDownload(uri)) match {
                case Some(factory) =>
                  Async[F].pure(((factory, uri, FileSystems.getDefault.getPath(localPath))) :: acc)
                case None =>
                  Async[F].raiseError[FileGroup](
                    new IllegalStateException(s"No blob client available to download $uri")
                  )
              }
          }
          .map(conf -> _)
      }

    for {
      groups <- Resource.eval(groupsF)

      // Instantiate blob clients per-factory (reuse instances across files that share a client).
      // We deduplicate factories and allocate each as a Resource so they're properly finalised.
      allFactories = groups.flatMap(_._2.map(_._1)).distinct
      factoryInstances <- allFactories
                            .traverse(factory => factory.mk.map(client => factory -> client))
                            .map(_.toMap)

      // Build initial Asset instances with NotDownloaded status.
      initialGroups = groups.map {
                        case (conf, files) =>
                          val assets = files.map {
                            case (factory, uri, localPath) =>
                              Asset[F](factoryInstances(factory), uri, localPath, NotDownloaded)
                          }
                          DownloadedGroup[F](conf, assets)
                      }

      // Download all assets, updating statuses.
      downloadedGroups <- Resource.eval {
                            initialGroups.parTraverse { group =>
                              group.assets
                                .parTraverse(downloadAsset(group.conf.schemaKey.name, _))
                                .map(newStatuses =>
                                  group.copy(
                                    assets = group.assets.zip(newStatuses).map {
                                      case (asset, newStatus) => asset.copy(status = newStatus)
                                    }
                                  )
                                )
                            }
                          }
    } yield DownloadedAssets(downloadedGroups)
  }

  def updateStream[F[_]: Async](
    assetRefresher: AssetRefresher[F],
    refreshPeriod: FiniteDuration
  ): Stream[F, Nothing] =
    Stream
      .fixedDelay[F](refreshPeriod)
      .evalTap(_ => assetRefresher.refresh)
      .drain

  private def downloadAsset[F[_]: Async](name: String, asset: Asset[F]): F[AssetSuccessfulStatus] = {
    val io = asset match {
      case Asset(client, uri, localPath, NotDownloaded | Md5(_)) =>
        Logger[F].debug(s"Fetching asset from $uri") >>
          client.get(uri).flatMap(processDownloadedContent(uri, localPath, _))
      case Asset(client, uri, localPath, Etag(oldEtag)) =>
        Logger[F].debug(s"Fetching asset from $uri if etag does not match $oldEtag") >>
          client.getIfNeeded(uri, oldEtag).flatMap[AssetSuccessfulStatus] {
            case BlobClient.EtagMatched =>
              Logger[F]
                .debug(s"Asset from $uri was not fetched because etag matched $oldEtag")
                .as(Etag(oldEtag))
            case result: BlobClient.GetResult =>
              processDownloadedContent(uri, localPath, result)
          }
    }
    retryingOnSomeErrors(retryPolicy[F], worthRetrying[F], onError[F](name, _, _))(io)
  }

  private def getMd5[F[_]: Async](content: ByteBuffer): F[String] =
    Stream
      .chunk(Chunk.byteBuffer(content))
      .through(Hashing.forSync[F].hash(HashAlgorithm.MD5))
      .compile
      .lastOrError
      .map(_.toString)

  // Uses writeOutputStream with FileOutputStream instead of NIO FileChannel (e.g. fs2
  // Files.writeAll) to avoid off-heap memory growth. FileChannel.write(heapByteBuffer) allocates a
  // temporary direct ByteBuffer in a thread-local cache (sun.nio.ch.Util$BufferCache) that is never
  // released.
  private def writeToPath[F[_]: Async](path: NioPath, content: ByteBuffer): F[Unit] =
    Stream
      .chunk(Chunk.byteBuffer(content))
      .through(fs2.io.writeOutputStream(Sync[F].blocking[OutputStream](new FileOutputStream(path.toFile))))
      .compile
      .drain

  private def processDownloadedContent[F[_]: Async](
    uri: URI,
    localPath: NioPath,
    result: BlobClient.GetResult
  ): F[AssetSuccessfulStatus] =
    result match {
      case BlobClient.ContentWithEtag(content, etag) =>
        for {
          _ <- writeToPath(localPath, content)
          _ <- Logger[F].info(s"Downloaded enrichment asset $uri with etag $etag to local path $localPath")
        } yield Etag(etag)
      case BlobClient.ContentNoEtag(content) =>
        for {
          md5 <- getMd5[F](content)
          _ <- writeToPath(localPath, content)
          _ <- Logger[F].info(s"Downloaded enrichment asset $uri with md5 $md5 to local path $localPath")
        } yield Md5(md5)
    }

  private def retryPolicy[F[_]: Applicative]: RetryPolicy[F] =
    RetryPolicies.fullJitter[F](1500.milliseconds).join(RetryPolicies.limitRetries[F](5))

  private def worthRetrying[F[_]: Applicative](e: Throwable): F[Boolean] =
    e match {
      case _: RetryableFailure => Applicative[F].pure(true)
      case _: IllegalArgumentException => Applicative[F].pure(false)
      case NonFatal(_) => Applicative[F].pure(false)
    }

  private def onError[F[_]: Sync](
    name: String,
    error: Throwable,
    retryDetails: RetryDetails
  ): F[Unit] =
    if (retryDetails.givingUp)
      Logger[F].error(
        show"Failed to download an asset for enrichment $name after ${retryDetails.retriesSoFar} retries: ${error.getMessage}. Aborting"
      )
    else if (retryDetails.retriesSoFar === 0)
      Logger[F].warn(show"Failed to download an asset for enrichment $name: ${error.getMessage}. Retrying")
    else
      Logger[F].warn(
        show"Failed to download an asset for enrichment $name after ${retryDetails.retriesSoFar} retries: ${error.getMessage}. " +
          show"Retrying in ${retryDetails.upcomingDelay}"
      )
}
