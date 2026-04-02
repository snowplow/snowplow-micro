/**
  * PROPRIETARY AND CONFIDENTIAL
  *
  * Unauthorized copying of this file via any medium is strictly prohibited.
  *
  * Copyright (c) 2018-2022 Snowplow Analytics Ltd. All rights reserved.
  */

import sbt._

object Dependencies {

  val resolvers = Seq(
    ("Snowplow Maven" at "http://maven.snplow.com/releases/").withAllowInsecureProtocol(true)
  )

  object V {
    // Snowplow
    val snowplowStreamCollector = "3.7.0"
    val snowplowCommonEnrich    = "6.10.0"
    val snowplowAnalyticsSdk    = "3.2.0"

    val http4sCirce = "0.23.23"
    val decline     = "2.4.1"
    val slf4j       = "2.0.17"
    val doobie      = "1.0.0-RC10"
    val postgresql  = "42.7.8"

    // specs2
    val specs2   = "4.12.2"
    val specs2CE = "1.5.0"

    // cloud SDKs
    val awsSdk           = "2.33.1"
    val gcpSdk           = "2.45.0"
    val azureStorageBlob = "12.33.2"
    val azureIdentity    = "1.18.2"
  }

  val snowplowStreamCollector = "com.snowplowanalytics" %% "snowplow-stream-collector-http4s-core" % V.snowplowStreamCollector
  val snowplowCommonEnrich    = "com.snowplowanalytics" %% "snowplow-common-enrich"                % V.snowplowCommonEnrich
  val snowplowAnalyticsSdk    = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk"          % V.snowplowAnalyticsSdk
  
  val http4sCirce  = "org.http4s"    %% "http4s-circe"        % V.http4sCirce
  val decline      = "com.monovore"  %% "decline-effect"      % V.decline
  val slf4jSimple  = "org.slf4j"     %  "slf4j-simple"        % V.slf4j

  // doobie
  val doobieCore           = "org.tpolecat" %% "doobie-core"           % V.doobie
  val doobieHikari         = "org.tpolecat" %% "doobie-hikari"         % V.doobie
  val doobiePostgres       = "org.tpolecat" %% "doobie-postgres"       % V.doobie
  val doobiePostgresCirce  = "org.tpolecat" %% "doobie-postgres-circe" % V.doobie

  // postgresql
  val postgresqlJdbc = "org.postgresql" % "postgresql" % V.postgresql

  // specs2
  val specs2   = "org.specs2"     %% "specs2-core"                % V.specs2   % Test
  val specs2CE = "org.typelevel"  %% "cats-effect-testing-specs2" % V.specs2CE % Test

  // cloud SDKs
  val awsS3              = "software.amazon.awssdk" % "s3"                   % V.awsSdk
  val awsSts             = "software.amazon.awssdk" % "sts"                  % V.awsSdk % Runtime
  val googleCloudStorage = "com.google.cloud"       % "google-cloud-storage" % V.gcpSdk
  val azureStorageBlob   = "com.azure"              % "azure-storage-blob"   % V.azureStorageBlob
  val azureIdentity      = "com.azure"              %  "azure-identity"      % V.azureIdentity

}
