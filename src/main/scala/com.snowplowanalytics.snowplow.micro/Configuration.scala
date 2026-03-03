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

import cats.data.{EitherT, ValidatedNel}
import cats.effect.IO
import cats.implicits._
import com.monovore.decline.{Argument, Opts}
import com.snowplowanalytics.iglu.client.IgluCirceClient
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.iglu.client.resolver.Resolver.ResolverConfig
import com.snowplowanalytics.iglu.client.resolver.registries.{JavaNetRegistryLookup, Registry}
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.collector.core.{Config => CollectorConfig}
import com.snowplowanalytics.snowplow.enrich.common.adapters.{CallrailSchemas, CloudfrontAccessLogSchemas, GoogleAnalyticsSchemas, HubspotSchemas, MailchimpSchemas, MailgunSchemas, MandrillSchemas, MarketoSchemas, OlarkSchemas, PagerdutySchemas, PingdomSchemas, SendgridSchemas, StatusGatorSchemas, UnbounceSchemas, UrbanAirshipSchemas, VeroSchemas, AdaptersSchemas => EnrichAdaptersSchemas}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.{AtomicFields, EnrichmentRegistry}
import com.snowplowanalytics.snowplow.enrich.common.enrichments.registry.EnrichmentConf
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, Config => TypesafeConfig}
import fs2.io.file.{Files, Path => FS2Path}
import io.circe.config.syntax.CirceConfigOps
import io.circe.generic.semiauto.deriveDecoder
import io.circe.config.syntax._
import io.circe.syntax.EncoderOps
import io.circe.{Decoder, Json, JsonObject}
import org.http4s.Uri
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import java.net.URI
import java.nio.file.{Path, Paths}
import scala.concurrent.duration.FiniteDuration

object Configuration {

  sealed trait OutputFormat
  object OutputFormat {
    case object None extends OutputFormat
    case object Tsv extends OutputFormat
    case object Json extends OutputFormat
  }

  sealed trait StorageMode
  object StorageMode {
    case object None extends StorageMode
    case object InMemory extends StorageMode
    case class Persistent(host: String,
                          port: Int,
                          database: String,
                          user: String,
                          password: String,
                          ttl: FiniteDuration,
                          cleanupInterval: FiniteDuration) extends StorageMode
  }

  object Cli {
    implicit val uriArgument: Argument[Uri] = Argument.from("uri") { str =>
      Uri.fromString(str).leftMap(_ => s"Invalid URI: $str").toValidatedNel
    }

    final case class Config(collectorAndEnrich: Option[Path],
                            iglu: Option[Path],
                            outputFormat: OutputFormat,
                            destination: Option[Uri],
                            yauaa: Boolean,
                            storage: StorageMode,
                            auth: Option[Path])

    private val collectorAndEnrich = (
      Opts.option[Path]("collector-config", "Configuration file for Collector (alias for --config)", metavar = "config.hocon") orElse
      Opts.option[Path]("config", "Configuration file for Collector and Enrich", "c", "config.hocon")
    ).orNone
    private val iglu = Opts.option[Path]("iglu", "Configuration file for Iglu Client", "i", "iglu.json").orNone
    private val outputTsv = Opts.flag("output-tsv", "Output events in TSV format to standard output or HTTP destination", "t").orFalse
    private val outputJson = Opts.flag("output-json", "Output events in JSON format to standard output or HTTP destination (with a separate key for each schema)", "j").orFalse
    private val destination = Opts.option[Uri]("destination", "HTTP(s) URL to send output data to (requires --output-json or --output-tsv)", "d").orNone
    private val noStorage = Opts.flag("no-storage", "Do not store events anywhere, and disable the API (handy if using Micro purely for output)").orFalse
    private val storage = Opts.option[Path]("storage", "Configuration file for PostgreSQL storage", "s", "storage.hocon").orNone
    private val yauaa = Opts.flag("yauaa", "Enable YAUAA user agent enrichment").orFalse
    private val auth = Opts.option[Path]("auth", "Configuration file for authentication", "a", "auth.hocon").orNone

    private val output = (outputTsv, outputJson, destination)
      .mapN { (_, _, _) }
      .mapValidated {
        case (true, false, d) => (OutputFormat.Tsv, d).validNel[String]
        case (false, true, d) => (OutputFormat.Json, d).validNel[String]
        case (false, false, None) => (OutputFormat.None, None).validNel[String]
        case (false, false, Some(_)) => "--destination requires either --output-tsv or --output-json".invalidNel[(OutputFormat, Option[Uri])]
        case (true, true, _) => "Cannot specify both --output-tsv and --output-json".invalidNel[(OutputFormat, Option[Uri])]
      }

    private val storageConfig = (noStorage, storage)
      .mapN { (_, _) }
      .mapValidated {
        case (true, _) =>
          StorageMode.None.validNel[String]
        case (_, None) =>
          StorageMode.InMemory.validNel[String]
        case (_, Some(path)) =>
          parseStorageConfig(path)
      }

    val config: Opts[Config] = (collectorAndEnrich, iglu, output, yauaa, storageConfig, auth).mapN {
      case (c, i, (f, d), y, s, a) => Config(c, i, f, d, y, s, a)
    }
  }


  object EnvironmentVariables {
    val igluRegistryUrl = "MICRO_IGLU_REGISTRY_URL"
    val igluApiKey = "MICRO_IGLU_API_KEY"
    val sslCertificatePassword = "MICRO_SSL_CERT_PASSWORD"
    val azureBlobAccount = "MICRO_AZURE_BLOB_ACCOUNT"
    val azureBlobSasToken = "MICRO_AZURE_BLOB_SAS_TOKEN"
    val postgresqlPassword = "MICRO_POSTGRESQL_PASSWORD"
  }

  final case class DummySinkConfig()

  type SinkConfig = DummySinkConfig
  implicit val dec: Decoder[DummySinkConfig] = Decoder.instance(_ => Right(DummySinkConfig()))

  final case class AuthConfig(domain: String,
                              apiDomain: String,
                              audience: String,
                              organizationId: String,
                              clientId: String)

  final case class StorageConfig(host: String,
                                port: Int,
                                database: String,
                                user: String,
                                ttl: FiniteDuration,
                                cleanupInterval: FiniteDuration)

  final case class MicroConfig(collector: CollectorConfig[SinkConfig],
                               iglu: IgluResources,
                               enrichmentsConfig: List[EnrichmentConf],
                               enrichConfig: EnrichConfig,
                               outputFormat: OutputFormat,
                               destination: Option[Uri],
                               storage: StorageMode,
                               auth: Option[AuthConfig])

  final case class EnrichValidation(atomicFieldsLimits: AtomicFields)
  final case class EnrichConfig(
    adaptersSchemas: EnrichAdaptersSchemas,
    maxJsonDepth: Int,
    validation: EnrichValidation,
    assetsUpdatePeriod: FiniteDuration,
    jsAllowedJavaClasses: Set[String]
  )

  final case class IgluResources(resolver: Resolver[IO], client: IgluCirceClient[IO])

  implicit private def logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private def parseStorageConfig(path: Path): ValidatedNel[String, StorageMode] = {
    try {
      val config = ConfigFactory.parseFile(path.toFile)
      config.as[StorageConfig] match {
        case Right(storageConfig) =>
          sys.env.get(EnvironmentVariables.postgresqlPassword) match {
            case Some(password) =>
              if (storageConfig.ttl.toMinutes < 5) {
                "Storage TTL must be at least 5 minutes (5m)".invalidNel[StorageMode]
              } else if (storageConfig.cleanupInterval.toMinutes < 1) {
                "Storage cleanup interval must be at least 1 minute (1m)".invalidNel[StorageMode]
              } else {
                StorageMode.Persistent(
                  storageConfig.host,
                  storageConfig.port,
                  storageConfig.database,
                  storageConfig.user,
                  password,
                  storageConfig.ttl,
                  storageConfig.cleanupInterval
                ).validNel[String]
              }
            case None =>
              s"PostgreSQL password not found in environment variable ${EnvironmentVariables.postgresqlPassword}".invalidNel[StorageMode]
          }
        case Left(error) =>
          s"Failed to parse storage configuration: ${error.show}".invalidNel[StorageMode]
      }
    } catch {
      case e: Exception =>
        s"Failed to read storage configuration file: ${e.getMessage}".invalidNel[StorageMode]
    }
  }

  def load(): Opts[EitherT[IO, String, MicroConfig]] = {
    Cli.config.map { cliConfig =>
      for {
        collectorConfig <- loadCollectorConfig(cliConfig.collectorAndEnrich)
        enrichConfig <- loadEnrichConfig(cliConfig.collectorAndEnrich)
        igluResources <- loadIgluResources(cliConfig.iglu, enrichConfig.maxJsonDepth)
        enrichmentsConfig <- loadEnrichmentConfig(igluResources.client, cliConfig.yauaa)
        authConfig <- loadAuthConfig(cliConfig.auth)
      } yield MicroConfig(
        collectorConfig, igluResources, enrichmentsConfig, enrichConfig,
        cliConfig.outputFormat, cliConfig.destination, cliConfig.storage, authConfig
      )
    }
  }

  private def loadCollectorConfig(path: Option[Path]): EitherT[IO, String, CollectorConfig[SinkConfig]] = {
    val resolveOrder = (config: TypesafeConfig) =>
      collectorNamespaced(ConfigFactory.load(
        collectorNamespaced(config.withFallback(
          collectorNamespaced(ConfigFactory.parseResources("collector-micro.conf")
            // collector-reference.conf only exists in fat jars
            // in Docker or sbt run, the fallback is correctly placed in the Collector jar
            .withFallback(ConfigFactory.parseResources("collector-reference.conf"))
          )
        ))
      ))

    loadConfig[CollectorConfig[SinkConfig]](path, resolveOrder)
  }

  private def loadIgluResources(path: Option[Path], maxJsonDepth: Int): EitherT[IO, String, IgluResources] = {
    val resolveOrder = (config: TypesafeConfig) =>
      config.withFallback(ConfigFactory.parseResources("default-iglu-resolver.conf"))

    loadConfig[ResolverConfig](path, resolveOrder)
      .flatMap(resolverConfig => buildIgluResources(resolverConfig, maxJsonDepth))
  }

  private def loadEnrichmentConfig(igluClient: IgluCirceClient[IO], enableYauaa: Boolean): EitherT[IO, String, List[EnrichmentConf]] = {
    val enrichments = Option(getClass.getResource("/enrichments")) match {
      case Some(definedEnrichments) =>
        val path = Paths.get(definedEnrichments.toURI)
        for {
          asJson <- loadEnrichmentsAsSDD(path, igluClient, fileType = ".json")
          asHocon <- loadEnrichmentsAsSDD(path, igluClient, fileType = ".hocon")
          asJSScripts <- loadJSScripts(path)
        } yield asJson ::: asHocon ::: asJSScripts
      case None =>
        EitherT.rightT[IO, String](List.empty[EnrichmentConf])
    }
    enrichments.map(maybeAddYauaa(_, enableYauaa))
  }

  /** If enableYauaa is true, add a new YAUAA enrichment config, unless another enabled one is already present */
  private def maybeAddYauaa(configs: List[EnrichmentConf], enableYauaa: Boolean) = {
    lazy val hasYauaa = configs.exists { conf =>
      conf.schemaKey.vendor == "com.snowplowanalytics.snowplow.enrichments" &&
        conf.schemaKey.name == "yauaa_enrichment_config"
    }

    if (!enableYauaa || hasYauaa) {
      configs
    } else {
      val yauaaEnrichment = EnrichmentConf.YauaaConf(
        schemaKey = SchemaKey("com.snowplowanalytics.snowplow.enrichments", "yauaa_enrichment_config", "jsonschema", SchemaVer.Full(1, 0, 0)),
        cacheSize = None
      )
      configs :+ yauaaEnrichment
    }
  }

  def loadEnrichConfig(configPath: Option[Path]): EitherT[IO, String, EnrichConfig] = {
    val resolveOrder = (config: TypesafeConfig) =>
      enrichNamespaced(ConfigFactory.load(
        enrichNamespaced(config.withFallback(
          enrichNamespaced(ConfigFactory.parseResources("enrich-micro.conf"))
        ))
      ))

    loadConfig[EnrichConfig](configPath, resolveOrder)
  }

  private def loadAuthConfig(authConfigPath: Option[Path]): EitherT[IO, String, Option[AuthConfig]] = {
    authConfigPath match {
      case Some(path) =>
        loadConfig[AuthConfig](Some(path), identity).map(Some(_))
      case None =>
        EitherT.rightT[IO, String](None)
    }
  }


  private def buildIgluResources(resolverConfig: ResolverConfig, maxJsonDepth: Int): EitherT[IO, String, IgluResources] =
    for {
      resolver <- Resolver.fromConfig[IO](resolverConfig).leftMap(_.show)
      completeResolver = resolver.copy(repos = resolver.repos ++ readIgluExtraRegistry())
      client <- EitherT.liftF(IgluCirceClient.fromResolver[IO](completeResolver, resolverConfig.cacheSize, maxJsonDepth))
    } yield IgluResources(completeResolver, client)

  private def loadEnrichmentsAsSDD(enrichmentsDirectory: Path,
                                   igluClient: IgluCirceClient[IO],
                                   fileType: String): EitherT[IO, String, List[EnrichmentConf]] = {
    listAvailableEnrichments(enrichmentsDirectory, fileType)
      .flatMap(loadEnrichmentsAsJsons)
      .map(asSDD)
      .flatMap(parseEnrichments(igluClient))
  }

  private def loadJSScripts(enrichmentsDirectory: Path): EitherT[IO, String, List[EnrichmentConf]] = EitherT.right {
    listFiles(enrichmentsDirectory, fileType = ".js")
      .flatMap { scripts =>
        scripts.traverse(buildJSConfig)
      }
  }

  private def buildJSConfig(script: FS2Path): IO[EnrichmentConf.JavascriptScriptConf] = {
    val schemaKey = SchemaKey("com.snowplowanalytics.snowplow", "javascript_script_config", "jsonschema", SchemaVer.Full(1, 0, 0))
    Files[IO]
      .readUtf8Lines(script)
      .compile
      .toList
      .map(lines => EnrichmentConf.JavascriptScriptConf(schemaKey, lines.mkString("\n"), JsonObject.empty))
  }

  private def listAvailableEnrichments(enrichmentsDirectory: Path, fileType: String) = {
    listFiles(enrichmentsDirectory, fileType)
      .attemptT
      .leftMap(e => show"Cannot list ${enrichmentsDirectory.toAbsolutePath.toString} directory with JSON: ${e.getMessage}")
  }

  private def listFiles(path: Path, fileType: String): IO[List[FS2Path]] = {
    Files[IO].list(fs2.io.file.Path.fromNioPath(path))
      .filter(path => path.toString.endsWith(fileType))
      .compile
      .toList
      .flatTap(files => logger.info(s"Files with extension: '$fileType' found in $path: ${files.mkString("[", ", ", "]")}"))
  }

  private def loadEnrichmentsAsJsons(enrichments: List[FS2Path]): EitherT[IO, String, List[Json]] = {
    enrichments.traverse { enrichmentPath =>
      loadConfig[Json](Some(enrichmentPath.toNioPath), identity)
    }
  }

  private def asSDD(jsons: List[Json]): SelfDescribingData[Json] = {
    val schema = SchemaKey("com.snowplowanalytics.snowplow", "enrichments", "jsonschema", SchemaVer.Full(1, 0, 0))
    SelfDescribingData(schema, Json.arr(jsons: _*))
  }

  private def parseEnrichments(igluClient: IgluCirceClient[IO])(sdd: SelfDescribingData[Json]): EitherT[IO, String, List[EnrichmentConf]] =
    EitherT {
      EnrichmentRegistry
        .parse[IO](sdd.asJson, igluClient, localMode = false, registryLookup = JavaNetRegistryLookup.ioLookupInstance[IO])
        .map(_.toEither)
    }.leftMap { x =>
      show"Cannot decode enrichments - ${x.mkString_(", ")}"
    }

  private def readIgluExtraRegistry(): Option[Registry.Http] = {
    sys.env.get(EnvironmentVariables.igluRegistryUrl).map { registry =>
      val uri = URI.create(registry)
      Registry.Http(
        Registry.Config(s"Custom ($registry)", 0, List.empty),
        Registry.HttpConnection(uri, sys.env.get(EnvironmentVariables.igluApiKey))
      )
    }
  }

  private def loadConfig[A: Decoder](path: Option[Path],
                                     load: TypesafeConfig => TypesafeConfig): EitherT[IO, String, A] = EitherT {
    IO {
      for {
        config <- Either.catchNonFatal(handleInputPath(path)).leftMap(_.getMessage)
        config <- Either.catchNonFatal(config.resolve()).leftMap(_.getMessage)
        config <- Either.catchNonFatal(load(config)).leftMap(_.getMessage)
        parsed <- config.as[A].leftMap(_.show)
      } yield parsed
    }
  }

  private def handleInputPath(path: Option[Path]): TypesafeConfig = {
    path match {
      case Some(definedPath) =>
        //Fail when provided file doesn't exist
        ConfigFactory.parseFile(definedPath.toFile, ConfigParseOptions.defaults().setAllowMissing(false))
      case None => ConfigFactory.empty()
    }
  }

  private def namespaced(config: TypesafeConfig, namespace: String): TypesafeConfig = {
    if (config.hasPath(namespace))
      config.getConfig(namespace).withFallback(config.withoutPath(namespace))
    else
      config
  }

  private def collectorNamespaced(config: TypesafeConfig) = namespaced(config, "collector")
  private def enrichNamespaced(config: TypesafeConfig) = namespaced(config, "enrich")

  implicit val resolverDecoder: Decoder[ResolverConfig] = Decoder.decodeJson.emap(json => Resolver.parseConfig(json).leftMap(_.show))

  implicit val enrichConfigDecoder: Decoder[EnrichConfig] =
    deriveDecoder[EnrichConfig]
  implicit val enrichAdaptersSchemasDecoder: Decoder[EnrichAdaptersSchemas] =
    deriveDecoder[EnrichAdaptersSchemas]
  implicit val callrailSchemasDecoder: Decoder[CallrailSchemas] =
    deriveDecoder[CallrailSchemas]
  implicit val cloudfrontAccessLogSchemasDecoder: Decoder[CloudfrontAccessLogSchemas] =
    deriveDecoder[CloudfrontAccessLogSchemas]
  implicit val googleAnalyticsSchemasDecoder: Decoder[GoogleAnalyticsSchemas] =
    deriveDecoder[GoogleAnalyticsSchemas]
  implicit val hubspotSchemasDecoder: Decoder[HubspotSchemas] =
    deriveDecoder[HubspotSchemas]
  implicit val mailchimpSchemasDecoder: Decoder[MailchimpSchemas] =
    deriveDecoder[MailchimpSchemas]
  implicit val mailgunSchemasDecoder: Decoder[MailgunSchemas] =
    deriveDecoder[MailgunSchemas]
  implicit val mandrillSchemasDecoder: Decoder[MandrillSchemas] =
    deriveDecoder[MandrillSchemas]
  implicit val marketoSchemasDecoder: Decoder[MarketoSchemas] =
    deriveDecoder[MarketoSchemas]
  implicit val olarkSchemasDecoder: Decoder[OlarkSchemas] =
    deriveDecoder[OlarkSchemas]
  implicit val pagerdutySchemasDecoder: Decoder[PagerdutySchemas] =
    deriveDecoder[PagerdutySchemas]
  implicit val pingdomSchemasDecoder: Decoder[PingdomSchemas] =
    deriveDecoder[PingdomSchemas]
  implicit val sendgridSchemasDecoder: Decoder[SendgridSchemas] =
    deriveDecoder[SendgridSchemas]
  implicit val statusgatorSchemasDecoder: Decoder[StatusGatorSchemas] =
    deriveDecoder[StatusGatorSchemas]
  implicit val unbounceSchemasDecoder: Decoder[UnbounceSchemas] =
    deriveDecoder[UnbounceSchemas]
  implicit val urbanAirshipSchemasDecoder: Decoder[UrbanAirshipSchemas] =
    deriveDecoder[UrbanAirshipSchemas]
  implicit val veroSchemasDecoder: Decoder[VeroSchemas] =
    deriveDecoder[VeroSchemas]

  implicit val validationDecoder: Decoder[EnrichValidation] =
    deriveDecoder[EnrichValidation]
  implicit val atomicFieldsDecoder: Decoder[AtomicFields] = Decoder[Map[String, Int]].emap { fieldsLimits =>
    val configuredFields = fieldsLimits.keys.toList
    val supportedFields = AtomicFields.supportedFields.map(_.name)
    val unsupportedFields = configuredFields.diff(supportedFields)

    if (unsupportedFields.nonEmpty)
      Left(s"""
        |Configured atomic fields: ${unsupportedFields.mkString("[", ",", "]")} are not supported.
        |Supported fields: ${supportedFields.mkString("[", ",", "]")}""".stripMargin)
    else
      Right(AtomicFields.from(fieldsLimits))
  }

  implicit val authConfigDecoder: Decoder[AuthConfig] = deriveDecoder[AuthConfig]

  implicit val storageConfigDecoder: Decoder[StorageConfig] = deriveDecoder[StorageConfig]
}
