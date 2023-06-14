package com.github.iboltaev.notifier.backend.net

import cats.effect.cps._
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import fs2.{Stream => FStream}
import org.http4s.{HttpRoutes, Response, Status}
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
  protected def handleWsReceive(clientId: String, receive: FStream[IO, Seq[WebSocketFrame]]): FStream[IO, Unit]
  protected def handleInitRoom(room: String): IO[Unit]

  private def groupFrames(stream: FStream[IO, WebSocketFrame]): FStream[IO, Seq[WebSocketFrame]] = {
    stream.scan(ReceiveScanner())(_ + _).map(_.get).filter(_.nonEmpty)
  }

  // TODO: clients map manipulation -> State; make normal logs!!!
  private def handleClose(clientId: String): IO[Unit] =
    state.update(_.removeClient(clientId))

  def routes(wsb: WebSocketBuilder[IO]) = {
    HttpRoutes.of[IO] {
      case GET -> Root / "init" / room => async[IO] {
        handleInitRoom(room).await
        Response(Status.Ok)
      }

      case GET -> Root / "ws"  => async[IO] {
        val clientId = UUID.randomUUID().toString

        val sendStream = FStream.bracket { async[IO] {
          val send = Queue.unbounded[IO, WebSocketFrame].await
          val client = Client(clientId)(send)
          state.getAndUpdate(_.addClient(clientId, client)).await

          send
        }} { _ =>
          handleClose(clientId)
        }.flatMap(q => FStream.fromQueueUnterminated(q))

        wsb.build(
          sendStream,
          rec => handleWsReceive(clientId, groupFrames(rec))).await
      }
    }
  }
}

