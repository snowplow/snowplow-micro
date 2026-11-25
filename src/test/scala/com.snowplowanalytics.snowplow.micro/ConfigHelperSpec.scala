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

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import com.monovore.decline.Command
import com.snowplowanalytics.snowplow.micro.Configuration.{MicroConfig, OutputFormat}
import org.specs2.mutable.Specification

class ConfigHelperSpec extends Specification with CatsEffect {
  "Configuration loader should work when" >> {
    "no custom args are provided and only defaults are used" >> {
      load(args = List.empty)
        .map { result =>
          result must beRight[MicroConfig].like {
            case config =>
              config.collector.port must beEqualTo(9090)
              config.collector.ssl.enable must beFalse 
              config.collector.ssl.port must beEqualTo(9543) 
              
              config.enrichmentsConfig.isEmpty must beTrue
              config.iglu.resolver.repos.map(_.config.name) must containTheSameElementsAs(List("Iglu Central", "Iglu Central - Mirror 01"))
              
              config.outputFormat must beEqualTo(OutputFormat.None)
          }
        }
    }
  }

  private def load(args: List[String]): IO[Either[String, MicroConfig]] = {
    Command("test-app", "test")(Configuration.load()).parse(args).toOption.get.value
  }
}
