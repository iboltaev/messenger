package com.github.iboltaev.notifier.backend.net

import cats.effect.cps._
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import fs2.{Stream => FStream}
import org.http4s.HttpRoutes
import org.http4s.dsl.io.{->, /, GET, Root}
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import org.http4s.websocket.WebSocketFrame.Continuation

import java.util.UUID

trait WebSocketSrv {
  case class ReceiveScanner(msgs: Seq[WebSocketFrame] = Seq.empty) {
    def +(frame: WebSocketFrame): ReceiveScanner = frame match {
      case Continuation(_, _) => ReceiveScanner(frame +: msgs)
      case _ => ReceiveScanner(Seq(frame))
    }

    def get: Seq[WebSocketFrame] = msgs.reverse
  }

  protected def state: Ref[IO, State]
  protected def handleReceive(room: String, clientId: String, receive: FStream[IO, Seq[WebSocketFrame]]): FStream[IO, Unit]

  protected def sendWs(room: String, clientId: String, ws: WebSocketFrame): IO[Boolean] = async[IO] {
    val st = state.get.await
    val clientOpt = st.rooms.get(clientId).flatMap(_.get(clientId))
    clientOpt.fold(IO(false)) { client =>
      // TODO: not just offer, get the ack from client and then return normal value!
      client.send.offer(ws).map(_ => true)
    }.await
  }

  private def groupFrames(stream: FStream[IO, WebSocketFrame]): FStream[IO, Seq[WebSocketFrame]] = {
    stream.scan(ReceiveScanner())(_ + _).map(_.get).filter(_.nonEmpty)
  }

  def routes(wsb: WebSocketBuilder[IO]) = {
    HttpRoutes.of[IO] {
      case GET -> Root / "ws" / room => async[IO] {
        val clientId = UUID.randomUUID().toString
        val send = Queue.unbounded[IO, WebSocketFrame].await

        val client = Client(clientId, room)(send)
        state.getAndUpdate { old =>
          val newRooms = old.rooms.get(room).fold {
            // crappy 'org.http4s.dsl.io.->' masks pair definition
            val pair = (room, Map((clientId, client)))
            old.rooms + pair
          } { r =>
            // crappy 'org.http4s.dsl.io.->' masks pair definition
            val pair = (clientId, client)
            old.rooms.updated(room, r + pair)
          }

          old.copy(rooms = newRooms)
        }.await

        wsb.build(
          FStream.fromQueueUnterminated(send),
          rec => handleReceive(room, clientId, groupFrames(rec) )).await
      }
    }
  }
}

