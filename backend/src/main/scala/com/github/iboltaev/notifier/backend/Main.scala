package com.github.iboltaev.notifier.backend

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.cps._
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder

import java.util.concurrent.TimeUnit

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] = async[IO] {
    val config = Config.config.await

    val res = MessengerService.makeEndpoints.await

    res.use { case (grpc, ws) =>
      IO.blocking {
        NettyServerBuilder
          .forPort(config.grpcPort)
          .keepAliveTime(5, TimeUnit.SECONDS)
          .addService(grpc)
          .build().start().awaitTermination()
      }
    }.await

    IO.consoleForIO.println(res).await

    ExitCode.Success
  }
}
