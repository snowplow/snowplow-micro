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
import com.google.cloud.storage.{StorageOptions, BlobId}
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder, BlobUrlParts}

import java.nio.ByteBuffer
import cats.effect.{IO, Resource, Sync}
import cats.implicits._
import com.azure.identity.DefaultAzureCredentialBuilder
import com.snowplowanalytics.snowplow.micro.Configuration.EnvironmentVariables
import fs2.{Stream, Chunk}
import fs2.io.file.{Files, Path}
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

sealed trait BlobClient {
  def mk: Resource[IO, BlobClientImpl]
}

class BlobClientImpl(download: URI => IO[ByteBuffer]) {
  def downloadToFiles(uriFilePairs: List[(URI, String)]): IO[Unit] = {
    uriFilePairs.traverse_ { case (uri, filePath) =>
      download(uri).flatMap { buffer =>
        Stream.chunk(Chunk.byteBuffer(buffer))
          .through(Files[IO].writeAll(Path(filePath)))
          .compile
          .drain
      }
    }
  }
}

case object S3Client extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      s3Client <- Resource.fromAutoCloseable(Sync[IO].delay(S3AsyncClient.builder().defaultsMode(DefaultsMode.AUTO).build()))
    } yield new BlobClientImpl(uri => {
      val bucket = uri.getHost
      val key = uri.getPath.stripPrefix("/")
      val request = GetObjectRequest.builder().bucket(bucket).key(key).build()

      Sync[IO].fromCompletableFuture(
        Sync[IO].delay(s3Client.getObject(request, AsyncResponseTransformer.toBytes[GetObjectResponse]()))
      ).map(response => ByteBuffer.wrap(response.asByteArrayUnsafe()))
    })
  }
}

case object GCSClient extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      service <- Resource.fromAutoCloseable(Sync[IO].delay(StorageOptions.getDefaultInstance.getService))
    } yield new BlobClientImpl(uri => {
      val bucket = uri.getHost
      val blobName = uri.getPath.stripPrefix("/")
      val blobId = BlobId.of(bucket, blobName)

      Sync[IO].blocking(service.readAllBytes(blobId))
        .map(ByteBuffer.wrap)
    })
  }
}

case class AzureClient(account: String) extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      client <- Resource.eval(createAzureClient(account))
    } yield new BlobClientImpl(uri => {
      val inputParts = BlobUrlParts.parse(uri.toString)
      val blobClient = client
        .getBlobContainerAsyncClient(inputParts.getBlobContainerName)
        .getBlobAsyncClient(inputParts.getBlobName)

      Sync[IO].fromCompletableFuture(
        Sync[IO].delay(blobClient.downloadContent().toFuture)
      ).map(binaryData => binaryData.toByteBuffer)
    })
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
      new BlobClientImpl(uri => {
        val request = Request[IO](Method.GET, Uri.unsafeFromString(uri.toString))

        httpClient.run(request).use { response =>
          response.status match {
            case Status.Ok =>
              response.body.compile.to(Array).map(ByteBuffer.wrap)
            case status =>
              IO.raiseError(new RuntimeException(s"Failed to download $uri: ${status.code} ${status.reason}"))
          }
        }
      })
    }
  }
}
