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

}

