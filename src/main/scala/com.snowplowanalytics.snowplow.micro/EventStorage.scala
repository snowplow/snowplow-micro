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

import cats.effect.{IO, Resource}
import com.snowplowanalytics.snowplow.micro.Configuration.MicroConfig

trait EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit]
  def addToBad(events: List[BadEvent]): IO[Unit]
  def reset(): IO[Unit]
}

object NoStorage extends EventStorage {
  def addToGood(events: List[GoodEvent]): IO[Unit] = IO.unit
  def addToBad(events: List[BadEvent]): IO[Unit] = IO.unit
  def reset(): IO[Unit] = IO.unit
}

object EventStorage {
  def create(config: MicroConfig): Resource[IO, EventStorage] = {
    (config.storage, config.maxEvents) match {
      case (_, Some(0)) =>
        Resource.pure(NoStorage)
      case (Some(storagePath), maxEvents) =>
        SqliteStorage.file(storagePath.toString, maxEvents)
      case (None, Some(maxEvents)) =>
        SqliteStorage.inMemory(Some(maxEvents))
      case (None, None) =>
        Resource.pure(new InMemoryStorage())
    }
  }
}