package com.github.iboltaev.notifier.backend

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.cps._

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = async[IO] {

    val res = MessengerService.makeEndpoints.await

    IO.consoleForIO.println(res).await

    ExitCode.Success
  }
}
