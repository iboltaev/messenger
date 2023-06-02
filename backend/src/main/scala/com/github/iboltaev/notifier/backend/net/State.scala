package com.github.iboltaev.notifier.backend.net

import cats.effect.IO
import cats.effect.std.Queue
import org.http4s.websocket.WebSocketFrame

case class Client(clientId: String, room: String)(private val sendQueue: Queue[IO, WebSocketFrame]) {
  def sendWs(frame: WebSocketFrame): IO[Boolean] = {
    // TODO: wait for response, not just offer and 'true'
    sendQueue.offer(frame).map(_ => true)
  }
}

case class State(rooms: Map[String, Map[String, Client]] = Map.empty)
