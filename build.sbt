/**
  * PROPRIETARY AND CONFIDENTIAL
  *
  * Unauthorized copying of this file via any medium is strictly prohibited.
  *
  * Copyright (c) 2019-2022 Snowplow Analytics Ltd. All rights reserved.
  */
import com.typesafe.sbt.packager.MappingsHelper.directory

import scala.collection.Seq

lazy val nashornJvmOptions = List(
  "-Dnashorn.args=--language=es6",
  "-Dnashorn.propertyMap.softReferenceDerivationLimit=0"
)

lazy val buildSettings = Seq(    
  name := "snowplow-micro",
  organization := "com.snowplowanalytics.snowplow",
  description := "Standalone application to automate testing of trackers",
  scalaVersion := "2.12.20",
  scalacOptions := Settings.compilerOptions,
  javacOptions := Settings.javaCompilerOptions,
  Runtime / unmanagedClasspath ++= Seq(
    baseDirectory.value / "config",
    baseDirectory.value / "ui" / "out"
  ),
  Test / parallelExecution := false,
  Test / javaOptions := nashornJvmOptions,
  Test / fork := true
)

lazy val dependencies = Seq(
  libraryDependencies ++= Seq(
    Dependencies.snowplowStreamCollector,
    Dependencies.snowplowCommonEnrich,
    Dependencies.snowplowAnalyticsSdk,
    Dependencies.decline,
    Dependencies.catsRetry,
    Dependencies.http4sCirce,
    Dependencies.http4sClient,
    Dependencies.doobieCore,
    Dependencies.doobieHikari,
    Dependencies.doobiePostgres,
    Dependencies.doobiePostgresCirce,
    Dependencies.postgresqlJdbc,
    Dependencies.specs2,
    Dependencies.specs2CE,
    Dependencies.slf4jSimple,
    Dependencies.awsS3,
    Dependencies.awsSts,
    Dependencies.googleCloudStorage,
    Dependencies.azureStorageBlob,
    Dependencies.azureIdentity
  ) ++ Dependencies.securityOverrides
)

lazy val buildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](name, moduleName, dockerAlias, version, "sinkName" -> "micro-ssc"),
  buildInfoPackage := "com.snowplowanalytics.snowplow.micro",
  buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.collector.core.AppInfo")
)


lazy val dynVerSettings = Seq(
  ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
  ThisBuild / dynverSeparator := "-"     // to be compatible with docker
)

lazy val commonSettings = 
  dependencies ++
  buildSettings ++
  buildInfoSettings ++
  dynVerSettings ++
  Settings.licenseSettings ++
  Settings.dynverOptions ++
  Settings.assemblyOptions

// Shared by both images; the Snowplow docker plugins supply everything else (base image, user, install location, ...)
lazy val dockerCommon = Seq(
  Docker / packageName := "snowplow-micro",
  Universal / mappings += file("LICENSE.md") -> "/SNOWPLOW-LICENSE.md",
  Universal / mappings ++= directory((ThisBuild / baseDirectory).value / "ui" / "out").map {
    case (source, destination) => (source, destination.replaceFirst("^out/", "ui/"))
  }
)

lazy val dockerSettings = Seq(
  scriptClasspath ++= Seq(s"${(Docker / defaultLinuxInstallLocation).value}/ui", "/config"),
  Universal / javaOptions ++= nashornJvmOptions
)

lazy val dockerSettingsDistroless = Seq(
  // Manifest Class-Path entries are URLs relative to lib/; directories need a trailing slash
  packageJavaLauncherJar / scriptClasspath ++= Seq("../ui/", "/config/"),
  dockerEntrypoint := {
    val orig = dockerEntrypoint.value
    orig.head +: nashornJvmOptions ++: orig.tail
  }
)

lazy val micro = project
  .in(file("."))
  .settings(commonSettings ++ dockerCommon ++ dockerSettings)
  .enablePlugins(BuildInfoPlugin, SnowplowDockerPlugin, JavaAppPackaging)
  
lazy val microDistroless = project
  .in(file("distroless/micro"))
  .settings(sourceDirectory := (micro / sourceDirectory).value)
  .settings(commonSettings ++ dockerCommon ++ dockerSettingsDistroless)
  .enablePlugins(BuildInfoPlugin, SnowplowDistrolessDockerPlugin, JavaAppPackaging)
