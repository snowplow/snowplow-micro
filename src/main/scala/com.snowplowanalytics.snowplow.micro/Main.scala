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

import cats.effect.{ExitCode, IO}
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp

object Main
  extends CommandIOApp(
    name = s"docker run ${BuildInfo.dockerAlias}",
    header =
      s"""Notes:
         |    - By default, events are stored in memory. This mode supports all API endpoints.
         |    - With --storage (PostgreSQL storage), /micro/all, /micro/good and /micro/bad endpoints are not supported.
         |    - With --no-storage (no in-memory or database storage), all /micro/* endpoints are disabled.""".stripMargin,
    version = BuildInfo.version
  ) {
  override def main: Opts[IO[ExitCode]] = Run.run()
}
