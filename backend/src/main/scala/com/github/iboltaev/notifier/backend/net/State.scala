package com.github.iboltaev.notifier.backend.net

import cats.effect.IO
import cats.effect.std.Queue
import org.http4s.websocket.WebSocketFrame

case class Client(clientId: String)(
  val sendQueue: Queue[IO, WebSocketFrame])
{
  def sendWs(frame: WebSocketFrame): IO[Boolean] = {
    // TODO: wait for response, not just offer and 'true'
    sendQueue.offer(frame).map(_ => true)
  }
}

case class State(allClients: Map[String, Client] = Map.empty) {
  def addClient(clientId: String, client: Client): State = {
    copy(allClients = allClients.updated(clientId, client))
  }

  def removeClient(clientId: String): State = {
    copy(allClients = allClients - clientId)
  }
}
