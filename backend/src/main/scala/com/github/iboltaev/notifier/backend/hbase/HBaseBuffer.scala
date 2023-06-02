package com.github.iboltaev.notifier.backend.hbase

import cats.effect.IO
import fs2.{Stream => FStream}
import com.github.iboltaev.notifier.BufferLogic
import com.github.iboltaev.notifier.BufferLogic.Msg
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{KeyCodec, ValueCodec}
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{KeyCodec, ValueCodec}

trait HBaseBuffer[M, E] extends HBaseClient with BufferLogic[M, E] {
  import HBaseBuffer._
  case class BufferStateKey(recipient: String)

  implicit val mValCodec: ValueCodec[M]

  private implicit lazy val stateKeyCodec = KeyCodec.gen[BufferStateKey]
  private implicit lazy val msgKeyCodec = KeyCodec.gen[MsgKey]
  private implicit lazy val stateValCodec = ValueCodec.gen[BufferState]
  private implicit lazy val msgValCodec = ValueCodec.gen[Msg]

  protected def bufferStateTableName: String
  protected def bufferStateColFamily: String = "cf"

  protected def bufferMessagesTableName: String
  protected def bufferMessagesColFamily: String = "cf"

  override protected def readBufferState(recipient: String): IO[BufferState] = {
    get[BufferStateKey, BufferState](bufferStateTableName, bufferStateColFamily, BufferStateKey(recipient + "-buf"))
  }

  override protected def casBufferState(recipient: String, expect: BufferState, newState: BufferState): IO[Boolean] = {
    cas[BufferStateKey, BufferState, BufferState](bufferStateTableName, bufferStateColFamily, BufferStateKey(recipient + "-buf"), expect, newState)
  }

  override protected def addMessage(recipient: String, msg: Msg): IO[Unit] = {
    put[MsgKey, Msg](bufferMessagesTableName, bufferMessagesColFamily, MsgKey(recipient, msg.epoch, msg.key, msg.timestamp), msg)
  }

  override protected def changeMessageEpoch(recipient: String, epoch: Long, timestamp: Long, key: String, value: M, toEpoch: Long): IO[Unit] = {
    del(bufferStateTableName, MsgKey(recipient, epoch, key, timestamp))
    put[MsgKey, Msg](bufferMessagesTableName, bufferMessagesColFamily, MsgKey(recipient, toEpoch, key, timestamp), Msg(toEpoch, key, timestamp, value))
  }

  override protected def readMessages(recipient: String, epoch: Long): FStream[IO, Msg] = {
    read[MsgKey, Msg](bufferMessagesTableName, bufferMessagesColFamily, MsgKey(recipient, epoch, "", 0), MsgKey(recipient, epoch + 1, "", 0))
      .map(_._2)
  }

  override def initBufferState(recipient: String): IO[Unit] = {
    putIfNotExists[BufferStateKey, BufferState](bufferStateTableName, bufferStateColFamily, BufferStateKey(recipient + "-buf"), "epoch", BufferLogic.State(0)).void
  }
}

object HBaseBuffer {
  case class MsgKey(recipient: String, epoch: Long, key: String, ts: Long)
}