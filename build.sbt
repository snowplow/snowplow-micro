/**
  * PROPRIETARY AND CONFIDENTIAL
  *
  * Unauthorized copying of this file via any medium is strictly prohibited.
  *
  * Copyright (c) 2019-2022 Snowplow Analytics Ltd. All rights reserved.
  */
import com.typesafe.sbt.packager.MappingsHelper.directory
import com.typesafe.sbt.packager.docker.*

import scala.collection.Seq

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
  Compile / unmanagedResources += file("LICENSE.md"),
  resolvers ++= Dependencies.resolvers,
  Test / parallelExecution := false,
  Test / javaOptions := Seq("-Dnashorn.args=--language=es6"),
  Test / fork := true
)

lazy val dependencies = Seq(
  libraryDependencies ++= Seq(
    Dependencies.snowplowStreamCollector,
    Dependencies.snowplowCommonEnrich,
    Dependencies.snowplowAnalyticsSdk,
    Dependencies.decline,
    Dependencies.http4sCirce,
    Dependencies.http4sClient,
    Dependencies.circeJawn,
    Dependencies.circeGeneric,
    Dependencies.specs2,
    Dependencies.specs2CE,
    Dependencies.badRows,
    Dependencies.slf4jSimple
  )
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

lazy val dockerCommon = Seq(
  Docker / maintainer := "Snowplow Analytics Ltd. <support@snowplow.io>",
  Docker / packageName := "snowplow/snowplow-micro",
  Docker / defaultLinuxInstallLocation := "/opt/snowplow",
  Docker / daemonUserUid := None,
  dockerPermissionStrategy := DockerPermissionStrategy.CopyChown,
  dockerRepository := Some("snowplow"),
  Universal / mappings += file("LICENSE.md") -> "/SNOWPLOW-LICENSE.md"
)

lazy val microSettingsDistroless = dockerCommon ++ Seq(
  dockerBaseImage := "gcr.io/distroless/java21-debian12:nonroot",
  Docker / daemonUser := "nonroot",
  Docker / daemonGroup := "nonroot",
  dockerEntrypoint := Seq(
    "java",
    "-Dnashorn.args=--language=es6",
    "-cp",
    s"/opt/snowplow/lib/${(packageJavaClasspathJar / artifactPath).value.getName}:/opt/snowplow/ui:/config",
    "com.snowplowanalytics.snowplow.micro.Main"
  ),
  Universal / mappings ++= directory((micro / baseDirectory).value / "ui" / "out").map {
    case (source, destination) => (source, destination.replaceFirst("^out/", "ui/"))
  },
  sourceDirectory := (micro / sourceDirectory).value
)

lazy val microSettings = dockerCommon ++ Seq(
  dockerBaseImage := "eclipse-temurin:21",
  Docker / daemonUser := "daemon",
  scriptClasspath ++= Seq("/opt/snowplow/ui", "/config"),
  Universal / javaOptions ++= Seq("-Dnashorn.args=--language=es6"),
  Universal / mappings ++= directory(baseDirectory.value / "ui" / "out").map {
    case (source, destination) => (source, destination.replaceFirst("^out/", "ui/"))
  }
)

lazy val micro = project
  .in(file("."))
  .settings(commonSettings ++ microSettings)
  .enablePlugins(BuildInfoPlugin, DockerPlugin, JavaAppPackaging)
  
lazy val microDistroless = project
  .in(file("distroless/micro"))
  .settings(commonSettings ++ microSettingsDistroless)
  .enablePlugins(BuildInfoPlugin, DockerPlugin, JavaAppPackaging, ClasspathJarPlugin)
