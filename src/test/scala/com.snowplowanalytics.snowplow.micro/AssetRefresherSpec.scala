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
import cats.effect.std.AtomicCell
import cats.effect.testing.specs2.CatsEffect

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowplow.micro.blob.{AssetRefresher, BlobClient}

import java.net.URI
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path => NioPath}

class AssetRefresherSpec extends Specification with CatsEffect {

  import AssetRefresher._
  import AssetRefresherSpec._

  "refresh" should {

    "not write the file nor hot-swap when the etag is unchanged" >> {
      withTempFile("v1") { localPath =>
        for {
          client <- FakeBlobClient.of(content = "v1", etag = Some("etag-1"))
          swaps <- Ref[IO].of(0)
          cell <- cellOf(client, localPath, Etag("etag-1"), swaps)
          _ <- new AssetRefresher(cell).refresh
          swapCount <- swaps.get
          fileContent <- readFile(localPath)
          calls <- client.calls.get
        } yield {
          swapCount must_== 0
          fileContent must_== "v1"
          calls.getIfNeeded must_== 1
          calls.get must_== 0
        }
      }
    }

    "write the new file and hot-swap when the etag changed" >> {
      withTempFile("v1") { localPath =>
        for {
          client <- FakeBlobClient.of(content = "v2", etag = Some("etag-2"))
          swaps <- Ref[IO].of(0)
          cell <- cellOf(client, localPath, Etag("etag-1"), swaps)
          _ <- new AssetRefresher(cell).refresh
          swapCount <- swaps.get
          fileContent <- readFile(localPath)
          status <- cell.get.map(_.head.assets.head.status)
        } yield {
          swapCount must_== 1
          fileContent must_== "v2"
          status must_== Etag("etag-2")
        }
      }
    }

    "fall back to MD5 when the source provides no etag, swapping only when content changes" >> {
      withTempFile("v1") { localPath =>
        for {
          client <- FakeBlobClient.of(content = "v1", etag = None)
          swaps <- Ref[IO].of(0)
          // status carries the MD5 of "v1" so an unchanged body is detected as unchanged
          v1Md5 <- md5Of("v1")
          cell <- cellOf(client, localPath, Md5(v1Md5), swaps)
          refresher = new AssetRefresher(cell)
          _ <- refresher.refresh
          swapsAfterUnchanged <- swaps.get
          _ <- client.setContent("v2")
          _ <- refresher.refresh
          swapsAfterChanged <- swaps.get
          fileContent <- readFile(localPath)
        } yield {
          swapsAfterUnchanged must_== 0
          swapsAfterChanged must_== 1
          fileContent must_== "v2"
        }
      }
    }

    "propagate the failure (crashing the app) when a download error is not retryable" >> {
      withTempFile("v1") { localPath =>
        for {
          client <- FakeBlobClient.failing(new IllegalArgumentException("boom"))
          swaps <- Ref[IO].of(0)
          cell <- cellOf(client, localPath, Etag("etag-1"), swaps)
          outcome <- new AssetRefresher(cell).refresh.attempt
        } yield outcome must beLeft
      }
    }
  }
}

object AssetRefresherSpec {

  import AssetRefresher._

  private val uri = URI.create("https://example.com/asset.db")

  private def bytes(s: String): ByteBuffer =
    ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))

  /** Builds a single-asset group whose `onUpdate` increments the given counter. */
  private def cellOf(
    client: FakeBlobClient,
    localPath: NioPath,
    status: AssetStatus,
    swaps: Ref[IO, Int]
  ): IO[AtomicCell[IO, List[EnrichmentAssetGroup[IO]]]] = {
    val asset = Asset[IO](client, uri, localPath, status)
    val group = EnrichmentAssetGroup[IO]("test-enrichment", List(asset), swaps.update(_ + 1))
    AtomicCell[IO].of(List(group))
  }

  private def withTempFile[A](initialContent: String)(f: NioPath => IO[A]): IO[A] = {
    val acquire = IO.blocking {
      val p = Files.createTempFile("micro-asset-refresher", ".tmp")
      Files.write(p, initialContent.getBytes(StandardCharsets.UTF_8))
      p
    }
    acquire.bracket(f)(p => IO.blocking(Files.deleteIfExists(p)).void)
  }

  private def readFile(path: NioPath): IO[String] =
    IO.blocking(new String(Files.readAllBytes(path), StandardCharsets.UTF_8))

  private def md5Of(s: String): IO[String] = {
    import fs2.hashing.{HashAlgorithm, Hashing}
    fs2.Stream
      .chunk(fs2.Chunk.byteBuffer(bytes(s)))
      .through(Hashing.forSync[IO].hash(HashAlgorithm.MD5))
      .compile
      .lastOrError
      .map(_.toString)
  }

  final case class Calls(get: Int, getIfNeeded: Int)

  /** In-memory blob client whose served content/etag can be mutated, and which counts its calls. */
  final class FakeBlobClient(
    state: Ref[IO, FakeBlobClient.State],
    val calls: Ref[IO, Calls],
    failWith: Option[Throwable]
  ) extends BlobClient[IO] {

    def setContent(content: String): IO[Unit] =
      state.update(_.copy(content = content))

    override def get(uri: URI): IO[BlobClient.GetResult] =
      calls.update(c => c.copy(get = c.get + 1)) >> raiseOr {
        state.get.map { s =>
          s.etag match {
            case Some(e) => BlobClient.ContentWithEtag(bytes(s.content), e)
            case None => BlobClient.ContentNoEtag(bytes(s.content))
          }
        }
      }

    override def getIfNeeded(uri: URI, etag: String): IO[BlobClient.GetIfNeededResult] =
      calls.update(c => c.copy(getIfNeeded = c.getIfNeeded + 1)) >> raiseOr {
        state.get.map { s =>
          s.etag match {
            case Some(e) if e == etag => BlobClient.EtagMatched
            case Some(e) => BlobClient.ContentWithEtag(bytes(s.content), e)
            case None => BlobClient.ContentNoEtag(bytes(s.content))
          }
        }
      }

    private def raiseOr[A](io: IO[A]): IO[A] =
      failWith match {
        case Some(t) => IO.raiseError(t)
        case None => io
      }
  }

  object FakeBlobClient {
    final case class State(content: String, etag: Option[String])

    def of(content: String, etag: Option[String]): IO[FakeBlobClient] =
      for {
        state <- Ref[IO].of(State(content, etag))
        calls <- Ref[IO].of(Calls(0, 0))
      } yield new FakeBlobClient(state, calls, None)

    def failing(t: Throwable): IO[FakeBlobClient] =
      for {
        state <- Ref[IO].of(State("", None))
        calls <- Ref[IO].of(Calls(0, 0))
      } yield new FakeBlobClient(state, calls, Some(t))
  }
}
