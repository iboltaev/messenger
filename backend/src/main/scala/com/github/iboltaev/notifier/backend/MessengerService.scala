package com.github.iboltaev.notifier.backend

import cats.effect.cps._
import cats.effect.unsafe.IORuntime
import cats.effect.{IO, Ref}
import cats.implicits.toTraverseOps
import com.github.iboltaev.messenger.requests.Requests
import com.github.iboltaev.notifier.BatchMessagingLogic.{Algo, FullMessage}
import com.github.iboltaev.notifier.backend.MessengerService.{Addr, Mess}
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.ValueCodec
import com.github.iboltaev.notifier.backend.hbase.{HBaseMessenger, fromJavaFuture}
import com.github.iboltaev.notifier.backend.net.messages.{InternalServiceFs2Grpc, Message, Messages, Response}
import com.github.iboltaev.notifier.backend.net.{InternalServiceSrv, WebSocketSrv}
import io.grpc.Metadata
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{AsyncConnection, ConnectionFactory}
import org.http4s.netty.server.NettyServerBuilder
import org.http4s.websocket.WebSocketFrame
import org.http4s.websocket.WebSocketFrame.Text

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

  private def mkAddr(room: String, clientId: String): Addr = {
    Addr(s"${config.host}:${config.grpcPort}:$clientId")
  }

  override protected def handleInitRoom(room: String): IO[Unit] = {
    initRoomIfNotExists(room)
  }

  // grpc
  // sends messages to appropriate clients, connected to this instance
  override def receive(request: Messages, ctx: Metadata): IO[Response] = async[IO]{
    val room = request.room
    val clientIds = request.clientIds.toSet
    val st = state.get.await
    val connectedClients = st.allClients

    // first failed part - clients that not connected
    val sub = clientIds.filterNot(connectedClients.contains)
    val taken = clientIds.filter(connectedClients.contains)

    // second failed part - clients with failed 'sendWs'
    taken.toSeq.flatMap(st.allClients.get).flatMap { client =>
      request.msgs.map { msg =>
        client.sendWs(Text(s"Message id=${msg.id} epoch=${msg.epoch} ts=${msg.timestamp} text=${msg.msg}"))
          .map(bool => (client.clientId, bool))
      }
    }.sequence.map { seq =>
      Response(sub.toSeq ++ seq.filter(_._2 == false).map(_._1))
    }.await
  }

  // websocket
  // TODO: make normal logging
  override protected def handleWsReceive(clientId: String, receive: fs2.Stream[IO, Seq[WebSocketFrame]]): fs2.Stream[IO, Unit] = {
    val textMsgs = receive.collect {
      case list @ Text(_, _) :: _ =>
        list.foldLeft(new StringBuilder) { (sb, f) =>
          sb.append(new String(f.data.toArray, "UTF-8"))
        }.result()
    }

    textMsgs.foreach { str =>
      Requests.parse(str) match {
        case Requests.RoomAddRequest(room) =>
          val ts = java.time.LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
          // signal about we're joining - put our address to buffer
          bufferSend(room, clientId, ts, Left(mkAddr(room, clientId)))
            .fold(IndexedSeq.empty[Algo[Mess]])(_ :+ _)
            .foreach { is =>
              IO.consoleForIO.println(s"Connected client $clientId, events: " + is)
            }.compile.drain

        case Requests.SendMessage(room, message) =>
          val msgId = UUID.randomUUID().toString

          send(room, Map(msgId -> Mess(message)), Set.empty)
            .fold(IndexedSeq.empty[Algo[Mess]])(_ :+ _)
            .foreach { is =>
              IO.consoleForIO.println(s"Send message $msgId, events: " + is)
            }.compile.drain

        case Requests.GetHistory(room, epoch, ts) =>
          // TODO: implement
          IO.unit
      }
    }
  }

  // from business logic
  override protected def sendToAll(room: String, addresses: Set[Addr], epoch: Long, messages: Map[String, FullMessage[Mess]]): IO[Set[Addr]] = {
    val groups = addresses.toSeq.map(_.adr.split(':')).groupBy(arr => (arr(0), arr(1))).map { case (tuple, addrs) =>
      val (host, port) = tuple

      val mess = new Messages(
        room,
        messages.toSeq.map { case (str, mess) =>
          new Message(str, mess.msg.message, mess.epoch, mess.timestamp)
        },
        addrs.map(_(2)))

      InternalServiceFs2Grpc.clientResource[IO, Any](
        NettyChannelBuilder.forAddress(host, port.toInt).usePlaintext().build(),
        _ => new Metadata()
      ).use { is =>
        is.receive(mess, ())
      }.map { resp =>
        resp.failedAddrs.map(clientId => Addr(s"$host:$port:$clientId"))
      }
    }

    groups.toSeq.sequence.map {
      _.flatten.toSet
    }
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
