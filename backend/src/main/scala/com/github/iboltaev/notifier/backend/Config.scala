package com.github.iboltaev.notifier.backend

import cats.effect.IO
import pureconfig._
import pureconfig.generic.auto._

case class Config(port: Int)

object Config {
  lazy val config: IO[Config] = IO.blocking {
    ConfigSource.default.load[Config]
      .right.get
  }
}
