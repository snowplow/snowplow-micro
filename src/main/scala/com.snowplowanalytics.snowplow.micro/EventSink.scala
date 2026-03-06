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

import cats.data.Validated
import cats.effect.IO
import cats.implicits._
import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, EventConverter}
import com.snowplowanalytics.snowplow.badrows.BadRow.{EnrichmentFailures, SchemaViolations, TrackerProtocolViolations}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure, Payload, Processor}
import com.snowplowanalytics.snowplow.collector.core.Sink
import com.snowplowanalytics.snowplow.enrich.common.EtlPipeline
import com.snowplowanalytics.snowplow.enrich.common.adapters.{AdapterRegistry, RawEvent}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{EnrichmentManager, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.loaders.ThriftLoader
import com.snowplowanalytics.snowplow.enrich.common.outputs.EnrichedEvent
import com.snowplowanalytics.snowplow.enrich.common.utils.{ConversionUtils, OptionIor}
import io.circe.syntax._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import com.snowplowanalytics.snowplow.micro.Configuration.{EnrichConfig, OutputFormat}
import org.http4s.client.Client
import org.http4s.{Method, Request, Uri}
import org.http4s.headers.`Content-Type`
import org.http4s.MediaType

import java.time.Instant

/** Sink of the collector that Snowplow Micro is.
 * Contains the functions that are called for each tracking event sent
 * to the collector endpoint.
 * The events are received as `CollectorPayload`s serialized with Thrift.
 * For each event it tries to validate it using Common Enrich,
 * and then stores the results in an [[EventStorage]].
 */
final class EventSink(igluClient: IgluCirceClient[IO],
                      registryLookup: RegistryLookup[IO],
                      enrichmentRegistry: EnrichmentRegistry[IO],
                      outputFormat: OutputFormat,
                      destination: Option[Uri],
                      processor: Processor,
                      enrichConfig: EnrichConfig,
                      httpClient: Client[IO],
                      private[micro] val storage: EventStorage) extends Sink[IO] {
  override val maxBytes = Int.MaxValue
  private lazy val logger = LoggerFactory.getLogger("EventLog")

  private val adapterRegistry = new AdapterRegistry[IO](Map.empty, enrichConfig.adaptersSchemas)

  override def isHealthy: IO[Boolean] = IO.pure(true)

  override def targetBytes: IO[Int] = IO.pure(maxBytes)

  override def storeRawEvents(events: List[Array[Byte]]): IO[Unit] = {
    events.traverse(collectEventsFromThriftBytes).map { results =>
      results.foldLeft((List.empty[GoodEvent], List.empty[BadEvent])) {
        case ((good, bad), (g, b)) => (g ::: good, b ::: bad)
      }
    }.flatMap { case (goodEvents, badEvents) =>
      storage.addToGood(goodEvents) >>
      storage.addToBad(badEvents) >>
      sendOutput(goodEvents)
    }
  }

  private def formatEvent(event: GoodEvent): String =
    s"id:${event.event.event_id}" +
      event.event.app_id.fold("")(i => s" app_id:$i") +
      event.eventType.fold("")(t => s" type:$t") +
      event.schema.fold("")(s => s" ($s)")

  private def formatBadRow(badRow: BadRow): String = badRow match {
    case TrackerProtocolViolations(_, Failure.TrackerProtocolViolations(_, _, _, messages), _) =>
      messages.map(_.asJson).toList.mkString
    case SchemaViolations(_, Failure.SchemaViolations(_, messages), _) =>
      messages.map(_.asJson).toList.mkString
    case EnrichmentFailures(_, Failure.EnrichmentFailures(_, messages), _) =>
      messages.map(_.message.asJson).toList.mkString
    case _ => "Error while validating the event."
  }

  /** Deserialize Thrift bytes into `CollectorPayload`s,
   * validate them and return the results for batch processing.
   * A `CollectorPayload` can contain several events.
   */
  private[micro] def collectEventsFromThriftBytes(thriftBytes: Array[Byte]): IO[(List[GoodEvent], List[BadEvent])] =
    ThriftLoader.toCollectorPayload(thriftBytes, processor, Instant.now()) match {
      case Validated.Valid(collectorPayload) =>
        adapterRegistry.toRawEvents(collectorPayload, igluClient, processor, registryLookup, enrichConfig.maxJsonDepth, Instant.now()).flatMap {
          case Validated.Valid(rawEvents) =>
            rawEvents.toList.foldLeftM((Nil, Nil): (List[GoodEvent], List[BadEvent])) {
              case ((good, bad), rawEvent) =>
                validateEvent(rawEvent).map {
                  case OptionIor.Right(goodEvent) =>
                    logger.info(s"GOOD ${formatEvent(goodEvent)}")
                    (goodEvent :: good, bad)
                  case OptionIor.Both((errors, badRow), goodEvent) =>
                    val badEvent = BadEvent(Some(collectorPayload), Some(rawEvent), errors)
                    logger.warn(s"BAD ${formatBadRow(badRow)}")
                    (goodEvent.copy(incomplete = true) :: good, badEvent :: bad)
                  case OptionIor.Left((errors, badRow)) =>
                    val badEvent = BadEvent(Some(collectorPayload), Some(rawEvent), errors)
                    logger.warn(s"BAD ${formatBadRow(badRow)}")
                    (good, badEvent :: bad)
                  case OptionIor.None =>
                    (good, bad)
                }
            }
          case Validated.Invalid(badRow) =>
            val bad = BadEvent(Some(collectorPayload), None, List("Error while extracting event(s) from collector payload and validating it/them.", badRow.compact))
            logger.warn(s"BAD ${bad.errors.head}")
            IO.pure((Nil, List(bad)))
        }
      case Validated.Invalid(badRows) =>
        val bad = BadEvent(None, None, List("Can't deserialize Thrift bytes.") ++ badRows.toList.map(_.compact))
        logger.warn(s"BAD ${bad.errors.head}")
        IO.pure((Nil, List(bad)))
    }

  /** Validate the raw event using Common Enrich logic, and extract the event type if any,
   * the schema if any, and the schemas of the contexts attached to the event if any.
   * @return [[GoodEvent]] with the extracted event type, schema and contexts,
   *   or error if the event couldn't be validated.
   */
  private[micro] def validateEvent(rawEvent: RawEvent): IO[OptionIor[(List[String], BadRow), GoodEvent]] =
    EnrichmentManager.enrichEvent[IO](
        enrichmentRegistry,
        igluClient,
        processor,
        DateTime.now(),
        rawEvent,
        EtlPipeline.FeatureFlags(acceptInvalid = false),
        IO.unit,
        registryLookup,
        enrichConfig.validation.atomicFieldsLimits,
        emitFailed = true,
        enrichConfig.maxJsonDepth
      ).map { result =>

        def validateToGoodEvent(enriched: EnrichedEvent): Validated[BadRow, GoodEvent] =
          EventConverter.fromEnriched(enriched) match {
            case Validated.Valid(converted) =>
              Validated.Valid(GoodEvent(rawEvent, converted.event, getEnrichedSchema(converted), getEnrichedContexts(converted), converted))
            case Validated.Invalid(failure) =>
              Validated.Invalid(BadRow.LoaderParsingError(processor, failure, Payload.RawPayload(ConversionUtils.tabSeparatedEnrichedEvent(enriched))))
          }

        def badResult(badRows: List[BadRow]): (List[String], BadRow) =
          ("Error while validating the event." :: badRows.map(_.compact), badRows.head)

        result match {
          case OptionIor.Left(badRow) =>
            OptionIor.Left(badResult(List(badRow)))
          case OptionIor.Right(enriched) =>
            validateToGoodEvent(enriched).fold(br => OptionIor.Left(badResult(List(br))), OptionIor.Right(_))
          case OptionIor.Both(badRow1, enriched) =>
            validateToGoodEvent(enriched)
              .fold(
                br => OptionIor.Left(badResult(List(badRow1, br))),
                ve => OptionIor.Both(badResult(List(badRow1)), ve)
              )
          case OptionIor.None =>
            OptionIor.None
        }
      }

  private def getEnrichedSchema(enriched: Event): Option[String] =
    List(enriched.event_vendor, enriched.event_name, enriched.event_format, enriched.event_version)
      .sequence
      .map(_.mkString("iglu:", "/", ""))

  private def getEnrichedContexts(enriched: Event): List[String] =
    enriched.contexts.data.map(_.schema.toSchemaUri)

  private def sendOutput(goodEvents: List[GoodEvent]): IO[Unit] = {
    if (goodEvents.nonEmpty) {
      (outputFormat, destination) match {
        case (OutputFormat.Tsv, Some(uri)) =>
          val tsvData = goodEvents.map(_.event.toTsv).mkString("\n")
          sendHttpRequest(uri, tsvData, MediaType.text.`tab-separated-values`)
        case (OutputFormat.Json, Some(uri)) =>
          val jsonData = goodEvents.map(_.event.toJson(lossy = true).noSpaces).mkString("\n")
          sendHttpRequest(uri, jsonData, MediaType.application.json)
        case (OutputFormat.Tsv, None) =>
          IO {
            goodEvents.foreach { event =>
              println(event.event.toTsv)
            }
          }
        case (OutputFormat.Json, None) =>
          IO {
            goodEvents.foreach { event =>
              println(event.event.toJson(lossy = true).noSpaces)
            }
          }
        case (OutputFormat.None, _) => IO.unit
      }
    } else {
      IO.unit
    }
  }

  private def sendHttpRequest(uri: Uri, data: String, mediaType: MediaType): IO[Unit] = {
    val request = Request[IO](
      method = Method.POST,
      uri = uri,
      headers = org.http4s.Headers(`Content-Type`(mediaType))
    ).withEntity(data)

    httpClient.run(request).use(_ => IO.unit).handleErrorWith { error =>
      IO(logger.warn(s"Failed to send data to destination $uri: ${error.getMessage}"))
    }
  }

}
