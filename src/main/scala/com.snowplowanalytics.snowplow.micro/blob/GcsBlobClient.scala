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

import com.google.cloud.storage.{Blob, BlobId, Storage, StorageException, StorageOptions}
import com.google.cloud.BaseServiceException

import cats.implicits._
import cats.effect.{Async, Resource, Sync}

import java.net.URI
import java.nio.ByteBuffer

import scala.language.higherKinds

private[blob] object GcsBlobClient {

  def client[F[_]: Async]: BlobClientFactory[F] =
    new BlobClientFactory[F] {

      override def canDownload(uri: URI): Boolean =
        uri.getScheme === "gs"

      override def mk: Resource[F, BlobClient[F]] =
        for {
          service <- Resource.fromAutoCloseable(Sync[F].delay(StorageOptions.getDefaultInstance.getService))
        } yield new BlobClient[F] {

          override def get(uri: URI): F[BlobClient.GetResult] = {
            val blobId = parseGcsUri(uri)
            val io: F[BlobClient.GetResult] = for {
              blob <- getBlob(service, blobId)
              content <- downloadContent(service, blobId, blob.getGeneration)
            } yield processBlobContent(blob, content)
            handleGcsErrors(io)
          }

          override def getIfNeeded(uri: URI, etag: String): F[BlobClient.GetIfNeededResult] = {
            val blobId = parseGcsUri(uri)
            val io: F[BlobClient.GetIfNeededResult] = for {
              blob <- getBlob(service, blobId)
              result <- Option(blob.getEtag) match {
                          case Some(currentEtag) if currentEtag === etag =>
                            // Etag matches - no need to download content
                            Sync[F].pure(BlobClient.EtagMatched: BlobClient.GetIfNeededResult)
                          case _ =>
                            // Etag doesn't match or is missing - download content
                            downloadContent(service, blobId, blob.getGeneration)
                              .map(content => processBlobContent(blob, content))
                        }
            } yield result
            handleGcsErrors(io)
          }
        }
    }

  private def parseGcsUri(uri: URI): BlobId = {
    val bucket = uri.getRawAuthority
    val blobName = uri.getPath.stripPrefix("/")
    BlobId.of(bucket, blobName)
  }

  private def getBlob[F[_]: Sync](service: Storage, blobId: BlobId): F[Blob] =
    Sync[F]
      .blocking(Option(service.get(blobId)))
      .flatMap {
        case None => Sync[F].raiseError(new Exception(s"Requested blob ${blobId.toGsUtilUri} is not found"))
        case Some(blob) => Sync[F].pure(blob)
      }

  private def downloadContent[F[_]: Sync](service: Storage, blobId: BlobId, generation: Long): F[ByteBuffer] =
    Sync[F]
      .blocking(service.readAllBytes(blobId, Storage.BlobSourceOption.generationMatch(generation)))
      .map(ByteBuffer.wrap)

  private def processBlobContent(blob: Blob, content: ByteBuffer): BlobClient.GetResult =
    Option(blob.getEtag) match {
      case Some(etag) => BlobClient.ContentWithEtag(content, etag)
      case None => BlobClient.ContentNoEtag(content)
    }

  private def handleGcsErrors[F[_]: Sync, A](io: F[A]): F[A] =
    io.adaptError {
      case se: StorageException if se.getCode === 412 =>
        // 412 Precondition Failed - blob changed between metadata and content fetch.
        // Retryable - the blob exists, it just changed.
        new RetryableFailure {
          override def getMessage: String = s"Blob generation mismatch (412): ${se.getMessage}"
          override def getCause: Throwable = se
        }
      case bse: BaseServiceException if bse.isRetryable =>
        new RetryableFailure {
          override def getMessage: String = bse.getMessage
          override def getCause: Throwable = bse
        }
    }
}
