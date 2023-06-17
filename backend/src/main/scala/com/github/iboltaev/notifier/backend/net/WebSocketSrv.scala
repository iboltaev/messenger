package com.github.iboltaev.notifier.backend.net

import cats.effect.cps._
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import fs2.{Stream => FStream}
import org.http4s.{HttpRoutes, Response, Status}
import org.http4s.dsl.io.{->, /, GET, Root}
import org.http4s.server.websocket.WebSocketBuilder
import org.http4s.websocket.WebSocketFrame
import org.http4s.websocket.WebSocketFrame.{Continuation, Ping, Pong}

import java.util.UUID
import scala.concurrent.duration.Duration

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

  private def groupFrames(stream: FStream[IO, WebSocketFrame], send: Queue[IO, WebSocketFrame]): FStream[IO, Seq[WebSocketFrame]] = {
    stream.scan(ReceiveScanner())(_ + _).map(_.get).filter(_.nonEmpty).evalMap { seq =>
      if (seq.head.isInstanceOf[Ping]) {
        send.offer(Pong()) >>
        IO(seq)
      } else {
        IO(seq)
      }
    }
  }

  // TODO: clients map manipulation -> State; make normal logs!!!
  private def handleClose(clientId: String): IO[Unit] =
    state.update(_.removeClient(clientId))

  private def sendPings(queue: Queue[IO, WebSocketFrame]): IO[Unit] =
    IO.sleep(Duration(15, "s")) >> queue.offer(Ping()) >> sendPings(queue)

  def routes(wsb: WebSocketBuilder[IO]) = {
    HttpRoutes.of[IO] {
      case GET -> Root / "init" / room => async[IO] {
        handleInitRoom(room).await
        Response(Status.Ok)
      }

      case GET -> Root / "ws"  => async[IO] {
        val clientId = UUID.randomUUID().toString
        val send = Queue.unbounded[IO, WebSocketFrame].await

        val sendStream = for {
          snd <- FStream.bracket { async[IO] {
            val client = Client(clientId)(send)
            state.getAndUpdate(_.addClient(clientId, client)).await
            send
          }} { _ =>
            handleClose(clientId)
          }
          _ <- FStream.bracket(sendPings(snd).start)(_.cancel)
          stream <- FStream.fromQueueUnterminated(snd)
        } yield stream

        wsb.build(
          sendStream,
          rec => handleWsReceive(clientId, groupFrames(rec, send))).await
      }
    }
  }
}

