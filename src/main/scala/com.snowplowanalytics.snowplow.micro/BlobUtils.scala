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

import software.amazon.awssdk.services.s3.S3AsyncClient
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import software.amazon.awssdk.core.async.AsyncResponseTransformer
import com.google.cloud.storage.{Storage, StorageOptions, BlobId}
import com.azure.core.http.HttpHeaderName
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder, BlobUrlParts}
import com.azure.storage.blob.models.{BlobRequestConditions, BlobStorageException}

import java.nio.ByteBuffer
import cats.effect.{IO, Resource, Sync}
import cats.implicits._
import com.azure.identity.DefaultAzureCredentialBuilder
import com.snowplowanalytics.snowplow.micro.Configuration.EnvironmentVariables

import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{Method, Request, Status, Uri}
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode

import java.net.URI

object BlobUtils {
  def blobClientFor(uri: URI): BlobClient = {
    uri.getScheme match {
      case "http" | "https" =>
        if (uri.getHost != null && uri.getHost.contains("blob.core.windows.net")) {
          AzureClient(uri.getHost.split("\\.").head)
        } else Http
      case "s3" => S3Client
      case "gs" => GCSClient
      case "abfss" =>
        // Extract account from abfss://container@account.dfs.core.windows.net/path
        val account = uri.getHost.split("@").last.split("\\.").head
        AzureClient(account)
      case scheme => throw new IllegalArgumentException(s"Unsupported URL scheme: $scheme")
    }
  }
}

/* Adapted and simplified from Enrich. TODO: Make Micro into an Enrich app and remove this. */

sealed trait DownloadResult
case class Downloaded(content: ByteBuffer, etag: Option[String]) extends DownloadResult
case object NotModified extends DownloadResult

sealed trait BlobClient {
  def mk: Resource[IO, BlobClientImpl]
}

class BlobClientImpl(
  download: (URI, Option[String]) => IO[DownloadResult]
) {
  def downloadIfNeeded(uri: URI, currentEtag: Option[String]): IO[DownloadResult] =
    download(uri, currentEtag)
}

case object S3Client extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      s3Client <- Resource.fromAutoCloseable(Sync[IO].delay(S3AsyncClient.builder().defaultsMode(DefaultsMode.AUTO).build()))
    } yield {
      val download: (URI, Option[String]) => IO[DownloadResult] = (uri, currentEtag) => {
        val bucket = uri.getHost
        val key = uri.getPath.stripPrefix("/")
        val builder = GetObjectRequest.builder().bucket(bucket).key(key)
        currentEtag.foreach(etag => builder.ifNoneMatch(etag))
        val request = builder.build()

        Sync[IO].fromCompletableFuture(
          Sync[IO].delay(s3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]()))
        ).map { response =>
          val etag = Option(response.response().eTag())
          Downloaded(ByteBuffer.wrap(response.asByteArrayUnsafe()), etag): DownloadResult
        }.recover {
          case e: software.amazon.awssdk.services.s3.model.S3Exception if e.statusCode() == 304 =>
            NotModified: DownloadResult
        }
      }

      new BlobClientImpl(download)
    }
  }
}

case object GCSClient extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      service <- Resource.fromAutoCloseable(Sync[IO].delay(StorageOptions.getDefaultInstance.getService))
    } yield {
      val download: (URI, Option[String]) => IO[DownloadResult] = (uri, currentEtag) => {
        val bucket = uri.getHost
        val blobName = uri.getPath.stripPrefix("/")
        val blobId = BlobId.of(bucket, blobName)

        Sync[IO].blocking(Option(service.get(blobId))).flatMap {
          case Some(blob) =>
            val remoteEtag = Option(blob.getEtag)
            if (currentEtag.isDefined && currentEtag == remoteEtag)
              IO.pure(NotModified: DownloadResult)
            else {
              val generation = blob.getGeneration
              Sync[IO].blocking(service.readAllBytes(blobId, Storage.BlobSourceOption.generationMatch(generation)))
                .map(bytes => Downloaded(ByteBuffer.wrap(bytes), remoteEtag): DownloadResult)
            }
          case None =>
            IO.raiseError(new RuntimeException(s"Blob not found: $uri"))
        }
      }

      new BlobClientImpl(download)
    }
  }
}

case class AzureClient(account: String) extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      client <- Resource.eval(createAzureClient(account))
    } yield {
      val download: (URI, Option[String]) => IO[DownloadResult] = (uri, currentEtag) => {
        val inputParts = BlobUrlParts.parse(uri.toString)
        val blobClient = client
          .getBlobContainerAsyncClient(inputParts.getBlobContainerName)
          .getBlobAsyncClient(inputParts.getBlobName)

        val conditions = currentEtag.map(etag => new BlobRequestConditions().setIfNoneMatch(etag)).orNull

        Sync[IO].fromCompletableFuture(
          Sync[IO].delay(blobClient.downloadContentWithResponse(null, conditions).toFuture)
        ).map { response =>
          val remoteEtag = Option(response.getHeaders.getValue(HttpHeaderName.ETAG))
          Downloaded(response.getValue.toByteBuffer, remoteEtag): DownloadResult
        }.recover {
          case e: BlobStorageException if e.getStatusCode == 304 =>
            NotModified: DownloadResult
        }
      }

      new BlobClientImpl(download)
    }
  }

  private def createAzureClient(account: String): IO[BlobServiceAsyncClient] = {
    Sync[IO].delay {
      val envVarAccount = sys.env.get(EnvironmentVariables.azureBlobAccount)
      val token = sys.env.get(EnvironmentVariables.azureBlobSasToken)

      val builder = new BlobServiceClientBuilder()
        .endpoint(s"https://$account.blob.core.windows.net")

      (envVarAccount, token) match {
        case (Some(`account`), Some(sasToken)) =>
          // Use SAS token if this account matches the configured one
          builder.sasToken(sasToken).buildAsyncClient()
        case _ =>
          // Use default credentials chain
          builder.credential(new DefaultAzureCredentialBuilder().build).buildAsyncClient()
      }
    }
  }
}

case object Http extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    EmberClientBuilder.default[IO].build.map { httpClient =>
      val download: (URI, Option[String]) => IO[DownloadResult] = (uri, currentEtag) => {
        val etagHeaders = currentEtag.toList.map(etag =>
          org.http4s.Header.Raw(org.typelevel.ci.CIString("If-None-Match"), etag)
        )
        val request = Request[IO](Method.GET, Uri.unsafeFromString(uri.toString), headers = org.http4s.Headers(etagHeaders))

        httpClient.run(request).use { response =>
          response.status match {
            case Status.Ok =>
              val etag = response.headers.get(org.typelevel.ci.CIString("ETag")).map(_.head.value)
              response.body.compile.to(Array).map(bytes => Downloaded(ByteBuffer.wrap(bytes), etag): DownloadResult)
            case Status.NotModified =>
              IO.pure(NotModified: DownloadResult)
            case status =>
              IO.raiseError(new RuntimeException(s"Failed to download $uri: ${status.code} ${status.reason}"))
          }
        }
      }

      new BlobClientImpl(download)
    }
  }
}
