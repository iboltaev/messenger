package com.github.iboltaev.notifier.backend.net

import cats.effect.cps._
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import fs2.{Stream => FStream}
import org.http4s.{HttpRoutes, Response, Status}
import org.http4s.dsl.io.{->, /, GET, Root}
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import org.http4s.websocket.WebSocketFrame.{Ping, Text}

import java.util.UUID

trait WebSocketSrv {
  protected def state: Ref[IO, State]
  protected def handleReceive(room: String, clientId: String, receive: FStream[IO, WebSocketFrame]): FStream[IO, Unit]

  protected def sendWs(room: String, clientId: String, ws: WebSocketFrame): IO[Unit] = async[IO] {
    val st = state.get.await
    val clientOpt = st.rooms.get(clientId).flatMap(_.get(clientId))
    clientOpt.fold(IO(())) { client =>
      client.send.offer(ws)
    }.await
  }

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

  def routes(wsb: WebSocketBuilder[IO]) = {
    HttpRoutes.of[IO] {
      case GET -> Root / "ws" / room => async[IO] {
        val clientId = UUID.randomUUID().toString
        val send = Queue.unbounded[IO, WebSocketFrame].await

        send.offer(Ping()).await
        send.offer(Text(s"Welcome $clientId")).await

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
          rec => handleReceive(room, clientId, rec )).await
      }
    }
  }
}

