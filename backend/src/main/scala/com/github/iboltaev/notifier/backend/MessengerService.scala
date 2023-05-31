package com.github.iboltaev.notifier.backend

import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref, Resource}
import cats.effect.cps._
import cats.effect.syntax.all._
import cats.effect.syntax.resource
import com.github.iboltaev.notifier.backend.MessengerService.{Addr, Mess}
import com.github.iboltaev.notifier.backend.hbase.{HBaseMessenger, fromJavaFuture}
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.ValueCodec
import com.github.iboltaev.notifier.backend.net.messages.{InternalServiceFs2Grpc, Messages, Response}
import com.github.iboltaev.notifier.backend.net.{InternalServiceSrv, WebSocketSrv}
import io.grpc.{Metadata, ServerServiceDefinition}
import fs2.grpc.syntax.all._
import org.http4s.netty.server.{NettyServerBuilder => WSNettyServerBuilder}
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{AsyncConnection, ConnectionFactory}
import org.http4s.server.Router
import org.http4s.websocket.WebSocketFrame

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

trait MessengerService
extends InternalServiceSrv
with WebSocketSrv
with HBaseMessenger[Addr, Mess] {
  protected def config: Config

  override implicit val msgValCodec: Codecs.ValueCodec[Mess] = ValueCodec.gen[Mess]
  override implicit val mValCodec: Codecs.ValueCodec[Either[Addr, Mess]] = ValueCodec.gen[Either[Addr, Mess]]
  override implicit val addressesValCodec: Codecs.ValueCodec[Set[Addr]] = new ValueCodec[Set[Addr]] {
    override def encodeMap(v: Set[Addr], param: String): Map[String, String] = Map((param -> v.map(_.adr).mkString(",")))
    override def decodeMap(m: Map[String, String], param: String): Set[Addr] = m(param).split(',').filterNot(_.isEmpty).map(Addr.apply).toSet
  }
  override protected def timeout: Duration = Duration.apply(config.bufferTimeout, "s")
  override implicit val runtime: IORuntime = IORuntime.global

  override protected def bufferStateTableName: String = config.bufferStateTable
  override protected def bufferMessagesTableName: String = config.bufferMessagesTable
  override protected def stateTableName: String = config.stateTable
  override protected def messagesTableName: String = config.messagesTable
  override protected def messageLogTableName: String = config.messageLogTable

  // grpc
  override def receive(request: Messages, ctx: Metadata): IO[Response] = ???

  // websocket
  override protected def handleReceive(room: String, clientId: String, receive: fs2.Stream[IO, WebSocketFrame]): fs2.Stream[IO, Unit] = ???

  // from business logic
  override protected def sendToAll(addresses: Set[Addr], epoch: Long, messages: Map[String, Mess]): IO[Set[Addr]] = ???
}

object MessengerService {
  case class Addr(adr: String)
  case class Mess(message: String)

  def makeMessengerService(hbaseSiteXml: String, appConfig: Config): IO[MessengerService] = async[IO] {
    IO.consoleForIO.println(appConfig).await

    val netState = Ref.of[IO, net.State](net.State()).await
    val conf = new Configuration()
    conf.addResource(hbaseSiteXml)
    val hbaseConnection = fromJavaFuture(ConnectionFactory.createAsyncConnection(conf)).await

    new MessengerService {
      override protected def config: Config = appConfig
      override protected def state: Ref[IO, net.State] = netState
      override def connection: AsyncConnection = hbaseConnection
    }
  }

  private def runServer(port: Int, service: ServerServiceDefinition): IO[Nothing] = {
    NettyServerBuilder
      .forPort(port)
      .keepAliveTime(5, TimeUnit.SECONDS)
      .addService(service)
      .resource[IO]
      .evalMap(server => IO.blocking(server.start()))
      .useForever
  }

  def makeEndpoints = async[IO] {
    import org.http4s.dsl.io.{->}
    import cats.syntax.all._

    val appConfig = Config.config.await

    val srvc = makeMessengerService("/hbase-site.xml", appConfig).await
    val service = InternalServiceFs2Grpc.bindServiceResource(srvc)

    val grpcHandle = service.use { ssd =>
      runServer(appConfig.grpcPort, ssd)
    }

    /*
    val wsHandle = WSNettyServerBuilder
      .apply[IO]
      .bindHttp(appConfig.wsPort, appConfig.host)
      .withHttpApp(
        Router(
          "/" -> srvc.routes
        ).orNotFound
      ).stream.compile.drain

    (grpcHandle, wsHandle).parMapN { case (_ , _) => () }
     */

    val res = grpcHandle.void.await
    res
  }
}
