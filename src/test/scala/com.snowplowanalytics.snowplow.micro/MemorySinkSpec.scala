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

import cats.implicits._
import cats.effect.testing.specs2.CatsResource
import cats.effect.{IO, Resource}
import org.http4s.ember.client.EmberClientBuilder
import io.circe.JsonObject
import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.{JavaNetRegistryLookup, Registry}
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}
import com.snowplowanalytics.snowplow.badrows.Processor
import com.snowplowanalytics.snowplow.enrich.common.enrichments.EnrichmentRegistry
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.JavascriptScriptEnrichment
import com.snowplowanalytics.snowplow.enrich.common.utils.OptionIor
import com.snowplowanalytics.snowplow.micro.Configuration.OutputFormat
import org.specs2.mutable.SpecificationLike

import scala.io.Source

class MemorySinkSpec extends CatsResource[IO, MemorySink] with SpecificationLike {

  import events._

  override val resource: Resource[IO, MemorySink] = createSink()

  sequential

  "processThriftBytes" >> {
    "should add a BadEvent to the cache if the array of bytes is not a valid Thrift payload" >> withResource { sink =>
      sink.validationCache.reset()
      val bytes = Array(1, 3, 5, 7).map(_.toByte)
      sink.processThriftBytes(bytes).map { _ =>
        sink.validationCache.filterBad() must beLike { case List(badEvent) if badEvent.errors.exists(_.contains("Can't deserialize Thrift bytes")) => ok }
        sink.validationCache.filterGood().size must beEqualTo(0)
      }
    }

    "should add a BadEvent to the cache if RawEvent(s) can't be extracted from the CollectorPayload" >> withResource { sink =>
      sink.validationCache.reset()
      val bytes = buildThriftBytesBadCollectorPayload()
      sink.processThriftBytes(bytes).map { _ =>
        sink.validationCache.filterBad() must beLike { case List(badEvent) if badEvent.errors.exists(_.contains("Error while extracting event(s) from collector payload")) => ok }
        sink.validationCache.filterGood().size must beEqualTo(0)
      }
    }

    "should add a GoodEvent and a BadEvent to the cache for a CollectorPayload containing both" >> withResource { sink =>
      sink.validationCache.reset()
      val bytes = buildThriftBytes1Good1Bad()
      sink.processThriftBytes(bytes).map { _ =>
        sink.validationCache.filterBad() must beLike { case List(badEvent) if badEvent.errors.exists(_.contains("Error while validating the event")) => ok }
        sink.validationCache.filterGood().size must beEqualTo(1)
      }
    }
  }

  "validateEvent" >> {
    "should fail if the event type parameter is not set" >> withResource { sink =>
      val raw = buildRawEvent()
      val withoutEvent = raw.copy(parameters = raw.parameters - "e")
      val expected = "Error while validating the event"
      sink.validateEvent(withoutEvent).value.map {
        _ must beLike {
          case OptionIor.Both((errors, _), _) if errors.exists(_.contains(expected)) => ok
        }
      }
    }

    "should fail for an invalid unstructured event" >> withResource { sink =>
      val raw = buildRawEvent(Some(buildUnstruct(sdjInvalid)))
      val expected = "Error while validating the event"
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Both((errors, _), _) if errors.exists(_.contains(expected)) => ok
        }
      }
    }

    "should fail if the event has an invalid context" >> withResource { sink =>
      val raw = buildRawEvent(None, Some(buildContexts(List(sdjInvalid))))
      val expected = "Error while validating the event"
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Both((errors, _), _) if errors.exists(_.contains(expected)) => ok
        }
      }
    }

    "should fail for a unstructured event with an unknown schema" >> withResource { sink =>
      val raw = buildRawEvent(Some(buildUnstruct(sdjDoesNotExist)))
      val expected = "Error while validating the event"
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Both((errors, _), _) if errors.exists(_.contains(expected)) => ok
        }
      }
    }

    "should fail if the event has a context with an unknown schema" >> withResource { sink =>
      val raw = buildRawEvent(None, Some(buildContexts(List(sdjDoesNotExist))))
      val expected = "Error while validating the event"
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Both((errors, _), _) if errors.exists(_.contains(expected)) => ok
        }
      }
    }

    "extract the type of an event" >> withResource { sink =>
      val raw = buildRawEvent()
      val expected = "page_ping"
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Right(GoodEvent(_, typE, _, _, _, _)) if typE === Some(expected) => ok
        }
      }
    }

    "should extract the schema of an unstructured event" >> withResource { sink =>
      val raw = buildRawEvent(Some(buildUnstruct(sdjLinkClick)))
      val expected = schemaLinkClick
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Right(GoodEvent(_, _, schema, _, _, _)) if schema === Some(expected) => ok
        }
      }
    }

    "should extract the contexts of an event" >> withResource { sink =>
      val raw = buildRawEvent(None, Some(buildContexts(List(sdjLinkClick, sdjMobileContext))))
      val expected = List(schemaLinkClick, schemaMobileContext)
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.Right(GoodEvent(_, _, _, contexts, _, _)) if contexts === expected => ok
        }
      }
    }

    "should return nothing if the event is dropped by JS enrichment" >> withResource { sink =>
      val raw = buildRawEvent(appId = "drop-all")
      sink.validateEvent(raw).value.map {
        _ must beLike {
          case OptionIor.None => ok
        }
      }
    }
  }

  private def createSink(): Resource[IO, MemorySink] = {
    for {
      enrichConfig <- Resource.eval(Configuration.loadEnrichConfig().value.map(_.getOrElse(throw new IllegalArgumentException("Can't read defaults from Enrich config"))))
      igluClient <- Resource.eval(IgluCirceClient.fromResolver[IO](Resolver[IO](List(Registry.IgluCentral), None), 500, enrichConfig.maxJsonDepth))
      jsEnrichment <- Resource.eval(buildJSEnrichment())
      enrichmentRegistry = new EnrichmentRegistry[IO](javascriptScript = List(jsEnrichment))
      processor = Processor(BuildInfo.name, BuildInfo.version)
      lookup = JavaNetRegistryLookup.ioLookupInstance[IO]
      httpClient <- EmberClientBuilder.default[IO].build
      validationCache = new ValidationCache(None)
    } yield new MemorySink(igluClient, lookup, enrichmentRegistry, OutputFormat.None, None, processor, enrichConfig, httpClient, validationCache)
  }

  private def buildJSEnrichment(): IO[JavascriptScriptEnrichment] = {
    val js = Source.fromResource("js-enrichment.js").getLines().mkString("\n")
    val key = SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", SchemaVer.Full(1, 0, 0))
    JavascriptScriptEnrichment.create(key, js, JsonObject(), false) match {
      case Right(enrichment) => IO.pure(enrichment)
      case Left(error) => IO.raiseError(new RuntimeException(error))
    }
  }

}
