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

import com.azure.core.http.HttpHeaderName
import com.azure.core.http.rest.Response
import com.azure.core.util.BinaryData
import com.azure.storage.blob.{BlobAsyncClient, BlobServiceAsyncClient, BlobServiceClientBuilder, BlobUrlParts}
import com.azure.storage.blob.models.{BlobRequestConditions, BlobStorageException}
import com.azure.identity.DefaultAzureCredentialBuilder

import cats.implicits._
import cats.effect.{Async, Resource, Sync}

import com.snowplowanalytics.snowplow.micro.Configuration.EnvironmentVariables

import java.net.URI

import scala.language.higherKinds

private[blob] object AzureBlobClient {

  def client[F[_]: Async]: BlobClientFactory[F] =
    new BlobClientFactory[F] {
      override def canDownload(uri: URI): Boolean =
        Option(uri.getHost).exists(_.contains("core.windows.net")) ||
          // abfss://container@account.dfs.core.windows.net/path
          uri.toString.contains("core.windows.net")

      override def mk: Resource[F, BlobClient[F]] =
        Resource.pure[F, BlobClient[F]] {
          new BlobClient[F] {

            override def get(uri: URI): F[BlobClient.GetResult] =
              blobAsyncClient(uri).flatMap { blobClient =>
                Async[F]
                  .fromCompletableFuture {
                    Sync[F].delay(blobClient.downloadContentWithResponse(null, null).toFuture)
                  }
                  .map(processResponse)
              }

            override def getIfNeeded(uri: URI, etag: String): F[BlobClient.GetIfNeededResult] =
              blobAsyncClient(uri).flatMap { blobClient =>
                val conditions = new BlobRequestConditions().setIfNoneMatch(etag)
                Async[F]
                  .fromCompletableFuture {
                    Sync[F].delay(blobClient.downloadContentWithResponse(null, conditions).toFuture)
                  }
                  .map[BlobClient.GetIfNeededResult](processResponse)
                  .recoverWith {
                    case bse: BlobStorageException if bse.getStatusCode === 304 =>
                      // 304 Not Modified - etag matched
                      Async[F].pure(BlobClient.EtagMatched)
                  }
              }
          }
        }
    }

  private def blobAsyncClient[F[_]: Sync](uri: URI): F[BlobAsyncClient] =
    Sync[F].delay {
      val inputParts = BlobUrlParts.parse(uri.toString)
      val account = inputParts.getAccountName
      serviceClient(account)
        .getBlobContainerAsyncClient(inputParts.getBlobContainerName)
        .getBlobAsyncClient(inputParts.getBlobName)
    }

  private def serviceClient(account: String): BlobServiceAsyncClient = {
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

  private def processResponse(response: Response[BinaryData]): BlobClient.GetResult = {
    val content = response.getValue.toByteBuffer
    Option(response.getHeaders.getValue(HttpHeaderName.ETAG)) match {
      case Some(etag) => BlobClient.ContentWithEtag(content, etag)
      case None => BlobClient.ContentNoEtag(content)
    }
  }
}
