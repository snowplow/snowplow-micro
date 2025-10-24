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
    val snowplowCommonEnrich    = "6.1.2"
    val snowplowAnalyticsSdk    = "3.2.0"
    val http4sCirce             = "0.23.23"

    val decline          = "2.4.1"
    val slf4j            = "2.0.17"

    // circe
    val circe = "0.14.2"

    // specs2
    val specs2        = "4.12.2"
    val specs2CE      = "1.5.0"

    // force versions of transitive dependencies
    val badRows   = "2.2.0"
  }

  val snowplowStreamCollector = "com.snowplowanalytics" %% "snowplow-stream-collector-http4s-core" % V.snowplowStreamCollector 
  val snowplowCommonEnrich    = "com.snowplowanalytics" %% "snowplow-common-enrich"                % V.snowplowCommonEnrich
  val snowplowAnalyticsSdk    = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk"          % V.snowplowAnalyticsSdk
  
  val http4sCirce  = "org.http4s"    %% "http4s-circe"        % V.http4sCirce
  val http4sClient = "org.http4s"    %% "http4s-ember-client" % V.http4sCirce
  val decline      = "com.monovore"  %% "decline-effect"      % V.decline
  val slf4jSimple  = "org.slf4j"     %  "slf4j-simple"        % V.slf4j

  // circe
  val circeJawn    = "io.circe" %% "circe-jawn"    % V.circe
  val circeGeneric = "io.circe" %% "circe-generic" % V.circe

  // specs2
  val specs2       = "org.specs2"     %% "specs2-core"                % V.specs2    % Test
  val specs2CE     = "org.typelevel"  %% "cats-effect-testing-specs2" % V.specs2CE  % Test

  // transitive
  val badRows          = "com.snowplowanalytics"           %% "snowplow-badrows"        % V.badRows
}
