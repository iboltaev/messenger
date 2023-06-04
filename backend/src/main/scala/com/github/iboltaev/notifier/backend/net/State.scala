package com.github.iboltaev.notifier.backend.net

import cats.effect.IO
import cats.effect.std.Queue
import org.http4s.websocket.WebSocketFrame

case class Client(clientId: String, room: String)(
  private val sendQueue: Queue[IO, WebSocketFrame])
{
  def sendWs(frame: WebSocketFrame): IO[Boolean] = {
    // TODO: wait for response, not just offer and 'true'
    sendQueue.offer(frame).map(_ => true)
  }
}

case class State(rooms: Map[String, Map[String, Client]] = Map.empty) {
  def addClient(room: String, clientId: String, client: Client): State = {
    val newRooms = rooms.get(room).fold {
      // crappy 'org.http4s.dsl.io.->' masks pair definition
      val pair = (room, Map((clientId, client)))
      rooms + pair
    } { r =>
      // crappy 'org.http4s.dsl.io.->' masks pair definition
      val pair = (clientId, client)
      rooms.updated(room, r + pair)
    }

    copy(rooms = newRooms)
  }

  def removeClient(room: String, clientId: String): State = {
    val newRooms = this.rooms.get(room)
    newRooms.fold(this) { map =>
      val nm = map - clientId
      if (nm.isEmpty)
        copy(rooms - room)
      else
        copy(rooms.updated(room, nm))
    }
  }
}
