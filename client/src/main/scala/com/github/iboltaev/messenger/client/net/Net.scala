package com.github.iboltaev.messenger.client.net

import cats.effect.IO
import cats.effect.std.{Dispatcher, Queue}
import fs2.{Stream => FStream}
import org.scalajs.dom.{Event, MessageEvent, WebSocket}

import scala.concurrent.duration.Duration

object Net {

  private def wsSocket(url: String, sendQueue: Queue[IO, Option[String]]): FStream[IO, Either[Event, MessageEvent]] = {
    val receiveStream = for {
      // make dispatcher and receive queue
      dispatcher <- FStream.resource(Dispatcher.sequential[IO])
      queue <- FStream.eval(Queue.unbounded[IO, Option[Either[Event, MessageEvent]]])
      // open new WebSocket
      ws <- FStream.bracket[IO, WebSocket] {
        IO.delay(new WebSocket(url))
      } { ws =>
        IO.delay(ws.close(0, "closed by client")).handleErrorWith(_ => IO.unit)
      }
      // make send fiber
      _ <- FStream.bracket {
        IO.consoleForIO.println("making send fiber...") >>
          FStream.fromQueueNoneTerminated(sendQueue).foreach { s =>
            IO.consoleForIO.println(s"sending $s") >> IO.delay(ws.send(s)).handleErrorWith { _ =>
              IO.consoleForIO.println(s"send failed: $s") >> sendQueue.offer(Some(s)) >> queue.offer(None)
            }
          }.compile.drain.start
      } { fiber =>
        IO.consoleForIO.println("send fiber cancel") >>
        fiber.cancel
      }
      // set websocket callbacks to write to the receive queue
      _ = {
        if (ws == null) queue.offer(None)
        else {
          ws.onopen = ev => dispatcher.unsafeRunAndForget(queue.offer(Some(Left(ev))))
          ws.onmessage = ev => dispatcher.unsafeRunAndForget(queue.offer(Some(Right(ev))))
          ws.onerror = _ => dispatcher.unsafeRunAndForget(IO.consoleForIO.println(s"Error??") >> queue.offer(None))
          ws.onclose = _ => dispatcher.unsafeRunAndForget(IO.consoleForIO.println(s"Closing??") >> queue.offer(None))
        }
      }
      // construct receive stream
      s <- FStream.fromQueueNoneTerminated(queue)
    } yield s

    receiveStream
  }

  def webSocketStream(url: String, sendQueue: Queue[IO, Option[String]], waitInterval: Int = 1): FStream[IO, Either[Event, MessageEvent]] = {
    (FStream.eval(IO.consoleForIO.println(s"Connecting to $url")) >> wsSocket(url, sendQueue)) ++
    (FStream.eval(IO.sleep(Duration.apply(waitInterval, "s"))) >> webSocketStream(url, sendQueue))
  }
}
