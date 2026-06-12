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

import cats.effect.{Async, Resource}

import org.http4s.client.{Client => Http4sClient}

import java.net.URI
import java.nio.ByteBuffer

import scala.language.higherKinds

/**
 * Factory for creating blob storage clients.
 * Implementations exist for S3, GCS, Azure Blob Storage, and HTTP(S).
 */
trait BlobClientFactory[F[_]] {

  /** Returns true if this client can download from the given URI scheme/host */
  def canDownload(uri: URI): Boolean

  /** Creates a resource-managed client implementation */
  def mk: Resource[F, BlobClient[F]]
}

/** Client for downloading blobs from remote storage. */
trait BlobClient[F[_]] {

  /**
   * Downloads a blob unconditionally.
   * @param uri The blob URI (e.g., s3://bucket/key, gs://bucket/path, https://...)
   * @return Downloaded content with optional etag
   */
  def get(uri: URI): F[BlobClient.GetResult]

  /**
   * Downloads a blob only if it has changed since the provided etag.
   * @param uri The blob URI
   * @param etag The previously known etag
   * @return EtagMatched if unchanged (e.g. 304 in HTTP response), or downloaded content if changed
   */
  def getIfNeeded(uri: URI, etag: String): F[BlobClient.GetIfNeededResult]
}

object BlobClient {

  /** Result of a conditional download attempt via getIfNeeded. */
  sealed trait GetIfNeededResult

  /**
   * The blob has not changed since the provided etag.
   * Server returned 304 Not Modified. No content was downloaded.
   */
  case object EtagMatched extends GetIfNeededResult

  /** Result of a successful blob download. */
  sealed trait GetResult extends GetIfNeededResult

  /**
   * Downloaded content without an etag.
   * Occurs when the storage backend does not provide etags (e.g., plain HTTP servers).
   * Future conditional requests cannot be made for this blob.
   */
  case class ContentNoEtag(content: ByteBuffer) extends GetResult

  /**
   * Downloaded content with an etag.
   * The etag can be used for future conditional requests via getIfNeeded.
   * Most blob storage services (S3, GCS, Azure) provide etags.
   */
  case class ContentWithEtag(content: ByteBuffer, etag: String) extends GetResult
}

/**
 * Marker trait for transient failures that should be retried.
 * Implementations throw exceptions of this type for network errors, 5xx responses, etc.
 */
private[blob] trait RetryableFailure extends Throwable

private[micro] object BlobUtils {

  /**
   * All blob client factories Micro knows about. The asset refresher selects the first one whose
   * `canDownload` returns true for a given asset URI.
   */
  def blobClientFactories[F[_]: Async](httpClient: Http4sClient[F]): List[BlobClientFactory[F]] =
    List(
      S3BlobClient.client[F],
      GcsBlobClient.client[F],
      AzureBlobClient.client[F],
      HttpBlobClient.wrapHttp4sClient[F](httpClient)
    )
}
