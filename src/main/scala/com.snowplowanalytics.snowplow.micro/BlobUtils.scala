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

import blobstore.azure.AzureStore
import blobstore.gcs.GcsStore
import blobstore.s3.S3Store
import blobstore.url.Url
import cats.effect.{IO, Resource, Sync}
import cats.implicits._
import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.storage.blob.{BlobServiceAsyncClient, BlobServiceClientBuilder}
import com.google.cloud.storage.StorageOptions
import com.snowplowanalytics.snowplow.micro.Configuration.EnvironmentVariables
import fs2.Stream
import fs2.io.file.{Files, Path}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{Method, Request, Status, Uri}
import software.amazon.awssdk.awscore.defaultsmode.DefaultsMode
import software.amazon.awssdk.services.s3.S3AsyncClient

import java.net.URI

object BlobUtils {
  def blobClientFor(uri: URI): BlobClient = {
    uri.getScheme match {
      case "http" | "https" =>
        if (uri.getHost != null && uri.getHost.contains("blob.core.windows.net")) {
          AzureClient(uri.getHost.split("\\.").head)
        } else Http
      case "s3" => S3Client
      case "gs" | "gcp" => GCSClient
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

class BlobClientImpl(download: URI => Stream[IO, Byte]) {
  def downloadToFiles(uriFilePairs: List[(URI, String)]): IO[Unit] = {
    uriFilePairs.traverse_ { case (uri, filePath) =>
      download(uri)
        .through(Files[IO].writeAll(Path(filePath)))
        .compile
        .drain
    }
  }
}

case object S3Client extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      s3Client <- Resource.fromAutoCloseable(Sync[IO].delay(S3AsyncClient.builder().defaultsMode(DefaultsMode.AUTO).build()))
      store <- Resource.eval(S3Store.builder[IO](s3Client).build.toEither.leftMap(_.head).pure[IO].rethrow)
    } yield new BlobClientImpl(uri => {
      Stream.eval(Url.parseF[IO](uri.toString)).flatMap { url =>
        store.get(url, 16 * 1024)
      }
    })
  }
}

case object GCSClient extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      service <- Resource.eval(Sync[IO].delay(StorageOptions.getDefaultInstance.getService))
      store <- Resource.eval(Sync[IO].delay(GcsStore.builder[IO](service).unsafe))
    } yield new BlobClientImpl(uri => {
      // Convert gcp:// to gs:// if needed
      val normalizedUri = if (uri.getScheme == "gcp") {
        new URI("gs", uri.getHost, uri.getPath, uri.getFragment)
      } else uri

      Stream.eval(Url.parseF[IO](normalizedUri.toString)).flatMap { url =>
        store.get(url, 16 * 1024)
      }
    })
  }
}

case class AzureClient(account: String) extends BlobClient {
  override def mk: Resource[IO, BlobClientImpl] = {
    for {
      client <- Resource.eval(createAzureClient(account))
      store <- Resource.eval(AzureStore.builder[IO](client).build.fold(
        errors => Sync[IO].raiseError(new RuntimeException(s"Failed to create Azure store for $account: ${errors.toList.mkString(", ")}")),
        s => Sync[IO].pure(s)
      ))
    } yield new BlobClientImpl(uri => {
      Stream.eval(Url.parseF[IO](uri.toString)).flatMap { url =>
        store.get(url, 16 * 1024)
      }
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

        Stream.eval(httpClient.run(request).use { response =>
          response.status match {
            case Status.Ok =>
              IO.pure(response.body)
            case status =>
              IO.raiseError(new RuntimeException(s"Failed to download $uri: ${status.code} ${status.reason}"))
          }
        }).flatten
      })
    }
  }
}
