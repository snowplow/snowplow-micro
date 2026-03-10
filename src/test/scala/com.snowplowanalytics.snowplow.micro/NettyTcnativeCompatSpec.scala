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

import org.specs2.mutable.Specification

/** Regression test for https://github.com/snowplow/snowplow-micro/issues/???
  *
  * azure-core-http-netty pulls reactor-netty into the classpath, which evicts
  * the AWS SDK's netty-handler:4.1.x in favour of 4.2.x.  netty-handler:4.2.x
  * calls SSL.setCurvesList() during TLS channel initialisation; that method was
  * added to netty-tcnative in 2.0.70.Final (the 4.1.x-era 2.0.65.Final does not
  * have it).  If dependencyOverrides for netty-tcnative are ever removed or
  * downgraded, every S3 download throws NoSuchMethodError before a single byte
  * is exchanged.
  */
class NettyTcnativeCompatSpec extends Specification {

  "netty-tcnative-boringssl-static on the classpath" should {
    "provide SSL.setCurvesList(long, String[]) required by netty-handler 4.2.x" >> {
      // initialize=false avoids running the static block that loads the BoringSSL native library,
      // which lets this test run on any platform (including macOS ARM in development).
      val sslClass = Class.forName("io.netty.internal.tcnative.SSL", false, getClass.getClassLoader)
      val method   = sslClass.getMethod("setCurvesList", classOf[Long], classOf[Array[String]])
      method must not(beNull)
    }
  }
}
