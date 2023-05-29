package com.github.iboltaev.notifier.backend

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.cps._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = async[IO] {
    net.InternalServiceSrv.serverHandle.await
    ExitCode.Success
  }
}
