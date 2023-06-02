package com.github.iboltaev.notifier.backend.hbase

import cats.effect.IO
import cats.effect.syntax.all._
import cats.implicits.toTraverseOps
import com.github.iboltaev.notifier.BatchMessagingLogic
import com.github.iboltaev.notifier.BatchMessagingLogic.MsgData
import fs2.{Stream => FStream}

import java.time.ZoneOffset

trait HBaseMessenger[A, M]
  extends HBaseBatchMessaging[A, M]
  with HBaseBuffer[Either[A, M], BatchMessagingLogic.Algo[M]]
{
   override protected def out(recipient: String, msgs: Seq[Msg]): FStream[IO, BatchMessagingLogic.Algo[M]] = {
     val (messages, addresses) = msgs.foldLeft((Map.empty[String, M], Set.empty[A])) { (s, msg) =>
       msg.msg.fold(a => s.copy(s._1, s._2 + a), b => s.copy(s._1 + (msg.key -> b), s._2))
     }

     val ts = java.time.LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
     val msgData = MsgData(recipient = recipient, messages = messages, timeStamp = ts)
     sendKeys(msgData, addresses)
   }

  def initRoomIfNotExists(room: String): IO[Unit] = {
    Seq(initBufferState(room), initState(room)).sequence.void
  }
}

