/**
  * PROPRIETARY AND CONFIDENTIAL
  *
  * Unauthorized copying of this file via any medium is strictly prohibited.
  *
  * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
  */

import sbt._

object Dependencies {

  object V {
    // Snowplow libraries
    val snowplowStreamCollector = "3.10.0"
    val snowplowCommonEnrich    = "6.15.0"
    val snowplowAnalyticsSdk    = "3.2.3"

    // Scala and FP core
    val http4s    = "0.23.39"
    val decline   = "2.6.2"
    val catsRetry = "3.1.3"

    // Database
    val doobie     = "1.0.0-RC12"
    val postgresql = "42.7.15"

    // Cloud SDKs
    val awsSdk           = "2.59.7"
    val gcpSdk           = "2.79.0"
    val azureStorageBlob = "12.37.2"
    val azureIdentity    = "1.19.0"

    // Logging
    val slf4j = "2.0.20"

    // Versions held above what something upstream asks for, to keep a vulnerable copy out of the
    // image. See securityOverrides below for which artifacts each one governs.
    val netty   = "4.2.21.Final"
    val mariadb = "3.4.5"

    // Held above upstream for binary compatibility rather than security: the netty override above
    // calls into a newer tcnative than the one its upstream brings in
    val tcnative = "2.0.86.Final"

    // Test only
    val specs2   = "4.23.0"
    val specs2CE = "1.8.0"
  }

  // Snowplow libraries
  val snowplowStreamCollector = "com.snowplowanalytics" %% "snowplow-stream-collector-http4s-core" % V.snowplowStreamCollector
  val snowplowCommonEnrich    = "com.snowplowanalytics" %% "snowplow-common-enrich"                % V.snowplowCommonEnrich
  val snowplowAnalyticsSdk    = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk"          % V.snowplowAnalyticsSdk

  // Scala and FP core
  val http4sCirce  = "org.http4s"       %% "http4s-circe"        % V.http4s
  val http4sClient = "org.http4s"       %% "http4s-ember-client" % V.http4s
  val decline      = "com.monovore"     %% "decline-effect"      % V.decline
  val catsRetry    = "com.github.cb372" %% "cats-retry"          % V.catsRetry

  // Database
  val doobieCore          = "org.tpolecat"   %% "doobie-core"           % V.doobie
  val doobieHikari        = "org.tpolecat"   %% "doobie-hikari"         % V.doobie
  val doobiePostgres      = "org.tpolecat"   %% "doobie-postgres"       % V.doobie
  val doobiePostgresCirce = "org.tpolecat"   %% "doobie-postgres-circe" % V.doobie
  val postgresqlJdbc      = "org.postgresql"  % "postgresql"            % V.postgresql

  // Cloud SDKs
  val awsS3              = "software.amazon.awssdk" % "s3"                   % V.awsSdk
  val awsSts             = "software.amazon.awssdk" % "sts"                  % V.awsSdk % Runtime
  val googleCloudStorage = "com.google.cloud"       % "google-cloud-storage" % V.gcpSdk
  val azureStorageBlob   = "com.azure"              % "azure-storage-blob"   % V.azureStorageBlob
  val azureIdentity      = "com.azure"              % "azure-identity"       % V.azureIdentity

  // Logging
  val slf4jSimple = "org.slf4j" % "slf4j-simple" % V.slf4j

  // Artifacts named only to force a version up; grouped into securityOverrides below
  val nettyAll        = "io.netty"                   % "netty-all"                       % V.netty
  val nettyTcnative   = "io.netty"                   % "netty-tcnative-boringssl-static" % V.tcnative
  val mariadbClient   = "org.mariadb.jdbc"           % "mariadb-java-client"             % V.mariadb

  // Test only
  val specs2   = "org.specs2"    %% "specs2-core"                % V.specs2   % Test
  val specs2CE = "org.typelevel" %% "cats-effect-testing-specs2" % V.specs2CE % Test

  // Security overrides. An entry earns its place only while it still evicts something: once the
  // upstream catches up, delete it. To check, drop the line and compare the resolved runtime
  // classpath before and after — reading the dependency tree for eviction markers is unreliable,
  // because sbt truncates long lines.
  //
  // common-enrich brings in netty-all and mariadb-java-client. tcnative is not itself vulnerable,
  // but has to move in step with netty, so it goes whenever the netty override goes.
  val securityOverrides = Seq(
    nettyAll,
    nettyTcnative,
    mariadbClient
  )

}
