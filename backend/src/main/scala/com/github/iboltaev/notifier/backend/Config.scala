package com.github.iboltaev.notifier.backend

import cats.effect.IO
import pureconfig._
import pureconfig.generic.auto._

case class Config(
                   host: String,
                   grpcPort: Int,
                   wsPort: Int,
                   bufferTimeout: Int,
                   zkQuorum: String,
                   bufferStateTable: String,
                   bufferMessagesTable: String,
                   stateTable: String,
                   messagesTable: String,
                   messageLogTable: String)

object Config {
  lazy val config: IO[Config] = IO.blocking {
    ConfigSource.default.load[Config]
      .right.get
  }
}
