package com.github.iboltaev.notifier.backend.net

import cats.effect.{IO, Resource}
import cats.effect.syntax.all._
import com.github.iboltaev.notifier.backend.Config
import com.github.iboltaev.notifier.backend.net.messages.{InternalServiceFs2Grpc, Messages, Response}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.{Metadata, ServerServiceDefinition}
import fs2.grpc.syntax.all._

import java.util.concurrent.TimeUnit

trait InternalServiceSrv extends InternalServiceFs2Grpc[IO, Metadata] {
  override def receive(request: Messages, ctx: Metadata): IO[Response] = ???
}

object InternalServiceSrv {
  private def runServer(port: Int, service: ServerServiceDefinition): IO[Nothing] = {
    NettyServerBuilder
      .forPort(50053)
      .keepAliveTime(5, TimeUnit.SECONDS)
      .addService(service)
      .resource[IO]
      .evalMap(server => IO.blocking(server.start()))
      .useForever
  }

  lazy val service: Resource[IO, ServerServiceDefinition] =
    InternalServiceFs2Grpc.bindServiceResource[IO](new InternalServiceSrv {})

  lazy val serverHandle: IO[Nothing] = service.use { service =>
    for {
      config <- Config.config
      port = config.port
      rs <- runServer(port, service)
    } yield rs
  }

}
