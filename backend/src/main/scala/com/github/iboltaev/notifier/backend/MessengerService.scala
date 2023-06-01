package com.github.iboltaev.notifier.backend

import cats.effect.cps._
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref}
import cats.implicits.toTraverseOps
import com.github.iboltaev.notifier.BatchMessagingLogic.Algo
import com.github.iboltaev.notifier.backend.MessengerService.{Addr, Mess}
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.ValueCodec
import com.github.iboltaev.notifier.backend.hbase.{HBaseMessenger, fromJavaFuture}
import com.github.iboltaev.notifier.backend.net.messages.{InternalServiceFs2Grpc, Messages, Response}
import com.github.iboltaev.notifier.backend.net.{InternalServiceSrv, WebSocketSrv}
import io.grpc.Metadata
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{AsyncConnection, ConnectionFactory}
import org.http4s.netty.server.NettyServerBuilder
import org.http4s.websocket
import org.http4s.websocket.WebSocketFrame
import org.http4s.websocket.WebSocketFrame.{Close, Continuation, Text}

import java.time.ZoneOffset
import java.util.UUID
import scala.concurrent.duration.Duration

trait MessengerService
  extends InternalServiceSrv
  with WebSocketSrv
  with HBaseMessenger[Addr, Mess]
{
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

  // TODO: move to 'WebSocketSrv'
  private def handleClose(room: String, clientId: String): IO[Unit] = state.update { old =>
    println(s"handleClose room=$room, clientId=$clientId")
    val rooms = old.rooms.get(room)
    rooms.fold(old) { map =>
      val nm = map - clientId
      if (nm.isEmpty)
        old.copy(old.rooms - room)
      else
        old.copy(old.rooms.updated(room, nm))
    }
  }

  private def mkAddr(room: String, clientId: String): Addr = {
    Addr(s"${config.host}:${config.grpcPort}:$room:$clientId")
  }

  // grpc
  override def receive(request: Messages, ctx: Metadata): IO[Response] = {
    request.msgs.map { m =>
      // TODO: make normal Address instead of Array[]
      val Array(_, _, room, clientId) = m.addr.split(':')
      sendWs(room, clientId, Text(m.msg)).map(res => (res, m.addr))
    }.sequence.map { seq =>
      Response.apply(seq.filter(_._1 == true).map(_._2))
    }
  }

  // websocket
  // TODO: make normal logging
  override protected def handleReceive(room: String, clientId: String, receive: fs2.Stream[IO, Seq[WebSocketFrame]]): fs2.Stream[IO, Unit] = {
    val ts = java.time.LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
    val msgId = UUID.randomUUID().toString
    // signal about we're joining - put our address to buffer
    val str = bufferSend(room, clientId, ts, Left(mkAddr(room, clientId)))
      .fold(IndexedSeq.empty[Algo[Mess]])(_ :+ _)
      .foreach { is =>
        IO.consoleForIO.println(s"Connected client $clientId, events: " + is)
      }

    // receive grouped websocket frames from client
    val rec = receive.foreach { seq =>
      if (seq.size == 1) {
        val msg = seq.head
        msg match {
          case Close(_) => handleClose(room, clientId)
          case _ => IO(())
        }
      } else {
        val data = seq.foldLeft(new StringBuilder) { (sb, f) =>
          sb.append(new String(f.data.toArray, "UTF-8"))
        }.result()

        send(room, Map(msgId -> Mess(data)), Set.empty)
          .fold(IndexedSeq.empty[Algo[Mess]])(_ :+ _)
          .foreach { is =>
            IO.consoleForIO.println(s"Send message $msgId, events: " + is)
          }.compile.drain
      }
    }

    str ++ rec
  }

  // from business logic
  override protected def sendToAll(addresses: Set[Addr], epoch: Long, messages: Map[String, Mess]): IO[Set[Addr]] = {
    /*
    val groups = addresses.toSeq.map(_.adr.split(':')).groupBy(arr => (arr(0), arr(1))).map { case (tuple, value) =>
      val (host, port) = tuple

    }

     */

    IO(Set.empty)
  }
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

  def makeEndpoints = async[IO] {
    val appConfig = Config.config.await

    val srvc = makeMessengerService("/hbase-site.xml", appConfig).await
    val service = InternalServiceFs2Grpc.bindServiceResource(srvc)

    val wsHandle = NettyServerBuilder
      .apply[IO]
      .bindHttp(appConfig.wsPort, appConfig.host)
      .withHttpWebSocketApp(srvc.routes(_).orNotFound)
      .resource

    for {
      grpc <- service
      ws <- wsHandle
    } yield (grpc, ws)
  }
}
