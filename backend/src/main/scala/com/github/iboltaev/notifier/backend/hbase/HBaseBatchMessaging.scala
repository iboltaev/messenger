package com.github.iboltaev.notifier.backend.hbase

import cats.effect.IO
import fs2.{Stream => FStream}
import com.github.iboltaev.notifier.BatchMessagingLogic
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{KeyCodec, ValueCodec}

import java.time.ZoneOffset

trait HBaseBatchMessaging[A, M] extends HBaseClient with BatchMessagingLogic[A, M] {
  case class StateKey(recipient: String)
  case class MsgKey(key: String)
  case class LogKey(recipient: String, epoch: Long, ts: Long, key: String)
  case class Epoch(epoch: Long)
  case class Stub(stub: Long)

  implicit val msgValCodec: ValueCodec[M]
  implicit val addressesValCodec: ValueCodec[Set[A]]

  implicit lazy val stateKeyCodec = KeyCodec.gen[StateKey]
  implicit lazy val msgKeyCodec = KeyCodec.gen[MsgKey]
  implicit lazy val logKeyCodec = KeyCodec.gen[LogKey]
  implicit lazy val stubValCodec = ValueCodec.gen[Stub]

  protected def stateTableName: String
  protected def stateColFamily: String = "cf"

  protected def messagesTableName: String
  protected def messagesColFamily: String = "cf"

  protected def messageLogTableName: String
  protected def messageLogColFamily: String = "cf"

  // state logic
  protected def readState(recipient: String): IO[State] = {
    get[StateKey, State](stateTableName, stateColFamily, StateKey(recipient))
  }

  protected def casState(recipient: String, expect: State, newState: State): IO[Boolean] = {
    cas[StateKey, Epoch, State](stateTableName, stateColFamily, StateKey(recipient), Epoch(expect.epoch), newState)
  }

  // log logic
  protected def readMessages(recipient: String, fromEpoch: Long, fromTS: Long, untilEpoch: Long): FStream[IO, M] = {
    val from = LogKey(recipient, fromEpoch, fromTS, "")
    val until = LogKey(recipient, untilEpoch, 0, "")
    read[LogKey, M](messagesTableName, messagesColFamily, from, until).map(_._2)
  }

  protected def writeMessages(messages: Map[String, M]): IO[Long] = {
    val ts = java.time.LocalDateTime.now().toEpochSecond(ZoneOffset.UTC)
    putAll[MsgKey, M](messagesTableName, messagesColFamily, messages.toSeq.map { case (str, m) =>
      MsgKey(str) -> m
    }).map(_ => ts)
  }

  protected def logMessages(recipient: String, epoch: Long, timestamp: Long, keys: Set[String]): IO[Unit] = {
    putAll[LogKey, Stub](
      messageLogTableName,
      messageLogColFamily,
      keys.toSeq.map(k => (LogKey(recipient, epoch, timestamp, k), Stub(0))))
  }

}
