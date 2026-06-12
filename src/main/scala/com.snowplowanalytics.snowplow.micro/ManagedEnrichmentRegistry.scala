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

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.language.higherKinds

import cats.implicits._

import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{AtomicCell, NonEmptyHotswap}

import fs2.Stream

import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf._
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.apirequest.ApiRequestEnrichment
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.pii.PiiPseudonymizerEnrichment
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.sqlquery.SqlQueryEnrichment
import com.snowplowanalytics.snowplow.enrich.common.utils.{HttpClient => CommonHttpClient}

import com.snowplowanalytics.snowplow.micro.blob.{AssetRefresher, BlobClientFactory}

/**
 * Manages enrichment instances with per-enrichment lifecycle control.
 *
 * Asset-backed enrichments are held in a `NonEmptyHotswap` so that when their underlying file asset
 * changes, only the affected enrichment is swapped rather than the entire registry being rebuilt.
 * Non-asset enrichments are held as plain fields because they never need swapping.
 *
 * Use `snapshot` to acquire a point-in-time `EnrichmentRegistry` for processing a batch of events.
 * Use `refreshStream` to start the background asset-refresh loop.
 *
 * Adapted from Enrich's `ManagedEnrichmentRegistry`.
 */
case class ManagedEnrichmentRegistry[F[_]: Async](
  // Asset-backed enrichments — each wrapped in a NonEmptyHotswap so they can be swapped
  // independently when the underlying file changes.
  ipLookups: Option[NonEmptyHotswap[F, IpLookupsEnrichment[F]]] = None,
  iab: Option[NonEmptyHotswap[F, IabEnrichment]] = None,
  asnLookups: Option[NonEmptyHotswap[F, AsnLookupsEnrichment]] = None,
  refererParser: Option[NonEmptyHotswap[F, RefererParserEnrichment]] = None,
  uaParser: Option[NonEmptyHotswap[F, UaParserEnrichment[F]]] = None,
  eventSpec: Option[NonEmptyHotswap[F, EventSpecEnrichment[F]]] = None,
  // Non-asset enrichments — plain fields, never swapped.
  apiRequest: Option[ApiRequestEnrichment[F]] = None,
  piiPseudonymizer: Option[PiiPseudonymizerEnrichment] = None,
  sqlQuery: Option[SqlQueryEnrichment[F]] = None,
  anonIp: Option[AnonIpEnrichment] = None,
  campaignAttribution: Option[CampaignAttributionEnrichment] = None,
  cookieExtractor: Option[CookieExtractorEnrichment] = None,
  currencyConversion: Option[CurrencyConversionEnrichment[F]] = None,
  eventFingerprint: Option[EventFingerprintEnrichment] = None,
  httpHeaderExtractor: Option[HttpHeaderExtractorEnrichment] = None,
  javascriptScript: List[JavascriptScriptEnrichment] = Nil,
  userAgentUtils: Option[UserAgentUtilsEnrichment] = None,
  weather: Option[WeatherEnrichment[F]] = None,
  yauaa: Option[YauaaEnrichment] = None,
  crossNavigation: Option[CrossNavigationEnrichment] = None,
  botDetection: Option[BotDetectionEnrichment] = None,
  private val assetRefresher: AssetRefresher[F]
) {

  /**
   * Acquires a snapshot of the current enrichment state as a plain `EnrichmentRegistry`.
   *
   * For each asset-backed enrichment, acquires a shared read lock on the hotswap so the enrichment
   * cannot be swapped while the batch is in-flight. The resource releases all locks when the batch
   * is done.
   */
  def snapshot: Resource[F, EnrichmentRegistry[F]] = {
    // Helper to get the current value from an Optional NonEmptyHotswap.
    // Uses .get (acquires shared read lock) so the enrichment cannot be swapped mid-batch.
    def getHs[A](hs: Option[NonEmptyHotswap[F, A]]): Resource[F, Option[A]] =
      hs match {
        case None => Resource.pure[F, Option[A]](None)
        case Some(h) => h.get.map(Some(_))
      }

    for {
      ipLookupsSnapshot <- getHs[IpLookupsEnrichment[F]](ipLookups)
      iabSnapshot <- getHs[IabEnrichment](iab)
      asnLookupsSnapshot <- getHs[AsnLookupsEnrichment](asnLookups)
      refererParserSnapshot <- getHs[RefererParserEnrichment](refererParser)
      uaParserSnapshot <- getHs[UaParserEnrichment[F]](uaParser)
      eventSpecSnapshot <- getHs[EventSpecEnrichment[F]](eventSpec)
    } yield EnrichmentRegistry[F](
      apiRequest = apiRequest,
      piiPseudonymizer = piiPseudonymizer,
      sqlQuery = sqlQuery,
      anonIp = anonIp,
      campaignAttribution = campaignAttribution,
      cookieExtractor = cookieExtractor,
      currencyConversion = currencyConversion,
      eventFingerprint = eventFingerprint,
      httpHeaderExtractor = httpHeaderExtractor,
      iab = iabSnapshot,
      ipLookups = ipLookupsSnapshot,
      asnLookups = asnLookupsSnapshot,
      javascriptScript = javascriptScript,
      refererParser = refererParserSnapshot,
      uaParser = uaParserSnapshot,
      userAgentUtils = userAgentUtils,
      weather = weather,
      yauaa = yauaa,
      crossNavigation = crossNavigation,
      botDetection = botDetection,
      eventSpec = eventSpecSnapshot
    )
  }

  /** Runs the asset-refresh loop, hot-swapping enrichments whenever their files change. */
  def refreshStream(refreshPeriod: FiniteDuration): Stream[F, Nothing] =
    AssetRefresher.updateStream(assetRefresher, refreshPeriod)
}

object ManagedEnrichmentRegistry {

  /**
   * Constructs a `ManagedEnrichmentRegistry` from a list of `EnrichmentConf`s, and wires up the
   * `AssetRefresher` hot-swap callbacks against the built registry.
   *
   * `blobClients` are used to download asset files to disk before the enrichments are initialised.
   * `AssetRefresher` is an internal implementation detail: it does not appear in the returned type.
   *
   * Returns `Right` on success or `Left(errorMessage)` if any enrichment fails to initialise.
   */
  def build[F[_]: Async](
    confs: List[EnrichmentConf],
    blobClients: List[BlobClientFactory[F]],
    apiEnrichmentClient: CommonHttpClient[F],
    sqlEC: ExecutionContext,
    exitOnJsCompileError: Boolean,
    jsAllowedJavaClasses: Set[String]
  ): Resource[F, Either[String, ManagedEnrichmentRegistry[F]]] = {

    // The fold accumulates directly into ManagedEnrichmentRegistry using .copy().
    // A no-op AssetRefresher is used as a placeholder and replaced at the end.
    val init: Resource[F, Either[String, ManagedEnrichmentRegistry[F]]] =
      Resource.eval(
        AtomicCell[F]
          .of(List.empty[AssetRefresher.EnrichmentAssetGroup[F]])
          .map(cell => Right(ManagedEnrichmentRegistry[F](assetRefresher = new AssetRefresher(cell))))
      )

    AssetRefresher.initialDownload(confs, blobClients).flatMap { downloadedAssets =>
      confs
        .foldLeft(init) { (accR, conf) =>
          accR.flatMap {
            case Left(e) => Resource.pure(Left(e))
            case Right(r) =>
              conf match {

                // ---- Asset-backed enrichments ----

                case c: IpLookupsConf =>
                  NonEmptyHotswap(Resource.eval(c.enrichment[F]))
                    .map(hs => Right(r.copy(ipLookups = Some(hs))))

                case c: IabConf =>
                  NonEmptyHotswap(Resource.eval(c.enrichment[F]))
                    .map(hs => Right(r.copy(iab = Some(hs))))

                case c: AsnLookupsConf =>
                  NonEmptyHotswap(Resource.eval(rethrowEither(c.enrichment[F].value)))
                    .map(hs => Right(r.copy(asnLookups = Some(hs))))

                case c: RefererParserConf =>
                  NonEmptyHotswap(Resource.eval(rethrowEither(c.enrichment[F].value)))
                    .map(hs => Right(r.copy(refererParser = Some(hs))))

                case c: UaParserConf =>
                  NonEmptyHotswap(Resource.eval(rethrowEither(c.enrichment[F].value)))
                    .map(hs => Right(r.copy(uaParser = Some(hs))))

                case c: EventSpecConf =>
                  NonEmptyHotswap(Resource.eval(rethrowEither(c.enrichment[F].value)))
                    .map(hs => Right(r.copy(eventSpec = Some(hs))))

                // ---- Non-asset enrichments ----

                case c: ApiRequestConf =>
                  Resource
                    .eval(c.enrichment[F](apiEnrichmentClient))
                    .map(e => Right(r.copy(apiRequest = Some(e))))

                case c: PiiPseudonymizerConf =>
                  Resource.pure(Right(r.copy(piiPseudonymizer = Some(c.enrichment))))

                case c: SqlQueryConf =>
                  Resource
                    .eval(c.enrichment[F](sqlEC))
                    .map(e => Right(r.copy(sqlQuery = Some(e))))

                case c: AnonIpConf =>
                  Resource.pure(Right(r.copy(anonIp = Some(c.enrichment))))

                case c: CampaignAttributionConf =>
                  Resource.pure(Right(r.copy(campaignAttribution = Some(c.enrichment))))

                case c: CookieExtractorConf =>
                  Resource.pure(Right(r.copy(cookieExtractor = Some(c.enrichment))))

                case c: CurrencyConversionConf =>
                  Resource
                    .eval(c.enrichment[F])
                    .map(e => Right(r.copy(currencyConversion = Some(e))))

                case c: EventFingerprintConf =>
                  Resource.pure(Right(r.copy(eventFingerprint = Some(c.enrichment))))

                case c: HttpHeaderExtractorConf =>
                  Resource.pure(Right(r.copy(httpHeaderExtractor = Some(c.enrichment))))

                case c: JavascriptScriptConf =>
                  c.enrichment(exitOnJsCompileError, jsAllowedJavaClasses) match {
                    case Right(e) => Resource.pure(Right(r.copy(javascriptScript = r.javascriptScript :+ e)))
                    case Left(err) => Resource.pure(Left(err))
                  }

                case c: UserAgentUtilsConf =>
                  Resource.pure(Right(r.copy(userAgentUtils = Some(c.enrichment))))

                case c: WeatherConf =>
                  Resource
                    .eval(rethrowEither(c.enrichment[F].value))
                    .map(e => Right(r.copy(weather = Some(e))))

                case c: YauaaConf =>
                  Resource.pure(Right(r.copy(yauaa = Some(c.enrichment))))

                case c: CrossNavigationConf =>
                  Resource.pure(Right(r.copy(crossNavigation = Some(c.enrichment))))

                case c: BotDetectionConf =>
                  Resource.pure(Right(r.copy(botDetection = Some(c.enrichment))))
              }
          }
        }
        .flatMap {
          case Left(err) => Resource.pure(Left(err))
          case Right(r) =>
            wireAssetRefresher(downloadedAssets, r).map(refresher => Right(r.copy(assetRefresher = refresher)))
        }
    }
  }

  private def rethrowEither[F[_]: Async, A](fa: F[Either[String, A]]): F[A] =
    fa.flatMap {
      case Right(a) => Async[F].pure(a)
      case Left(err) => Async[F].raiseError(new RuntimeException(err))
    }

  private def wireAssetRefresher[F[_]: Async](
    downloaded: AssetRefresher.DownloadedAssets[F],
    registry: ManagedEnrichmentRegistry[F]
  ): Resource[F, AssetRefresher[F]] = {
    val groups: List[AssetRefresher.EnrichmentAssetGroup[F]] = downloaded.groups.flatMap { dg =>
      val onUpdateOpt: Option[F[Unit]] = dg.conf match {
        case c: IpLookupsConf =>
          registry.ipLookups.map(hs => hs.swap(Resource.eval(c.enrichment[F])))
        case c: IabConf =>
          registry.iab.map(hs => hs.swap(Resource.eval(c.enrichment[F])))
        case c: AsnLookupsConf =>
          registry.asnLookups.map(hs => hs.swap(Resource.eval(rethrowEither(c.enrichment[F].value))))
        case c: RefererParserConf =>
          registry.refererParser.map(hs => hs.swap(Resource.eval(rethrowEither(c.enrichment[F].value))))
        case c: UaParserConf =>
          registry.uaParser.map(hs => hs.swap(Resource.eval(rethrowEither(c.enrichment[F].value))))
        case c: EventSpecConf =>
          registry.eventSpec.map(hs => hs.swap(Resource.eval(rethrowEither(c.enrichment[F].value))))
        case _ => None
      }
      onUpdateOpt.map(onUpdate => AssetRefresher.EnrichmentAssetGroup[F](dg.conf.schemaKey.name, dg.assets, onUpdate))
    }
    Resource.eval(AtomicCell[F].of(groups)).map(new AssetRefresher(_))
  }
}
