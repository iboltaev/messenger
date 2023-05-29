package com.github.iboltaev.notifier.backend.net

import cats.effect.IO
import cats.effect.std.Queue
import org.http4s.websocket.WebSocketFrame

case class Client(clientId: String, room: String)(val send: Queue[IO, WebSocketFrame])

case class State(rooms: Map[String, Map[String, Client]])
