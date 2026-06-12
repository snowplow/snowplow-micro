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

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, S3Exception}
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import software.amazon.awssdk.core.ResponseBytes
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode

import cats.implicits._
import cats.effect.{Async, Resource, Sync}

import java.net.URI
import java.nio.ByteBuffer

import scala.language.higherKinds

private[blob] object S3BlobClient {

  def client[F[_]: Async]: BlobClientFactory[F] =
    new BlobClientFactory[F] {
      override def canDownload(uri: URI): Boolean =
        uri.getScheme === "s3"

      override def mk: Resource[F, BlobClient[F]] =
        for {
          s3Client <- Resource.fromAutoCloseable(
                        Sync[F].delay(S3AsyncClient.builder().defaultsMode(DefaultsMode.AUTO).build())
                      )
        } yield new BlobClient[F] {

          override def get(uri: URI): F[BlobClient.GetResult] = {
            val (bucket, key) = parseS3Uri(uri)
            val request = GetObjectRequest.builder().bucket(bucket).key(key).build()
            Async[F]
              .fromCompletableFuture {
                Sync[F].delay(s3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]()))
              }
              .map(processResponseBytes)
          }

          override def getIfNeeded(uri: URI, etag: String): F[BlobClient.GetIfNeededResult] = {
            val (bucket, key) = parseS3Uri(uri)
            val request = GetObjectRequest.builder().bucket(bucket).key(key).ifNoneMatch(etag).build()
            Async[F]
              .fromCompletableFuture {
                Sync[F].delay(s3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]()))
              }
              .map[BlobClient.GetIfNeededResult](processResponseBytes)
              .recover {
                case e: S3Exception if e.statusCode() === 304 =>
                  // 304 Not Modified - etag matched
                  BlobClient.EtagMatched
              }
          }
        }
    }

  private def parseS3Uri(uri: URI): (String, String) = {
    val bucket = uri.getHost
    val key = uri.getPath.stripPrefix("/")
    (bucket, key)
  }

  private def processResponseBytes(responseBytes: ResponseBytes[GetObjectResponse]): BlobClient.GetResult = {
    val response = responseBytes.response()
    val arr = responseBytes.asByteArrayUnsafe
    val content = ByteBuffer.wrap(arr, 0, arr.length)
    Option(response.eTag()) match {
      case Some(etag) => BlobClient.ContentWithEtag(content, etag)
      case None => BlobClient.ContentNoEtag(content)
    }
  }
}
