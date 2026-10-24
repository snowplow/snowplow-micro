/*
 * Copyright (c) 2019-2022 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
import sbt._
import Keys._

// SBT Assembly
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.Assembly.{JarEntry, Library}

// SBT DynVer
import sbtdynver.DynVerPlugin.autoImport._

import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._

object Settings {
  lazy val licenseSettings = Seq(
    licenses += ("Snowplow Limited Use License Agreement", url("https://docs.snowplow.io/limited-use-license-1.1")),
    headerLicense := Some(HeaderLicense.Custom(
      """|Copyright (c) 2019-present Snowplow Analytics Ltd. All rights reserved.
         |
         |This software is made available by Snowplow Analytics, Ltd.,
         |under the terms of the Snowplow Limited Use License Agreement, Version 1.1
         |located at https://docs.snowplow.io/limited-use-license-1.1
         |BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
         |OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
         |""".stripMargin
    )),
    headerMappings := headerMappings.value + (HeaderFileType.conf -> HeaderCommentStyle.hashLineComment),
    Compile / unmanagedResources += file("LICENSE.md")
  )

  lazy val compilerOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-Ywarn-dead-code",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-value-discard",
    "-Ypartial-unification",
    "-Wconf:cat=other-match-analysis:error"
  )

  lazy val javaCompilerOptions = Seq(
    "-source", "21",
    "-target", "21",
    "-Xlint"
  )

  lazy val assemblyOptions = Seq(
    assembly / assemblyOutputPath := {
      val dir = crossTarget.value / "assembled_jars"
      IO.createDirectory(dir)
      dir / s"${name.value}-${version.value}.jar"
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => referenceConfMergeStrategy
      case x => MergeStrategy.first
    }
  )

  /**
   * Micro loads the collector's reference.conf explicitly as `collector-reference.conf`, and relies on
   * enrich's being the one and only `reference.conf`. The common-streams reference.conf only has
   * defaults for streams sources and sinks, which micro does not use, so it gets discarded.
   *
   * Any other reference.conf fails the build, so that we decide how to handle it.
   */
  private lazy val referenceConfMergeStrategy = CustomMergeStrategy("ReferenceConf", 1) { dependencies =>
    val (collector, others) = dependencies.partition {
      case Library(module, _, _, _) => module.name.startsWith("snowplow-stream-collector")
      case _                        => false
    }
    val kept = others.filterNot {
      case Library(module, _, _, _) => module.organization == "com.snowplowanalytics" && module.name.startsWith("streams-")
      case _                        => false
    }
    kept match {
      case Vector(single) =>
        Right(collector.map(dep => JarEntry(s"collector-${dep.target}", dep.stream)) :+ JarEntry(single.target, single.stream))
      case _ =>
        Left(s"Expected exactly one reference.conf after handling collector and common-streams, but found: ${kept.mkString(", ")}")
    }
  }

  lazy val dynverOptions = Seq(
    ThisBuild / dynverSeparator := "-",    // to be compatible with docker
    ThisBuild / dynverTagPrefix := "micro-",
  )
}
