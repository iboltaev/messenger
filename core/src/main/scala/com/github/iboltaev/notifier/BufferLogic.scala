package com.github.iboltaev.notifier

import cats.effect.IO
import cats.effect.cps._
import com.github.iboltaev.notifier.BufferLogic._
import fs2.{Stream => FStream}

import scala.concurrent.duration.Duration

trait BufferLogic[M, E] {
  type BufferState = BufferLogic.State
  type Msg = BufferLogic.Msg[M]
  // delay
  protected def timeout: Duration
  // state stuff
  protected def readBufferState(recipient: String): IO[BufferState]
  protected def casBufferState(recipient: String, expect: BufferState, newState: BufferState): IO[Boolean]
  // messages stuff
  protected def addMessage(recipient: String, msg: Msg): IO[Unit]
  protected def changeMessageEpoch(recipient: String, epoch: Long, timestamp: Long, key: String, value: M, toEpoch: Long): IO[Unit]
  protected def readMessages(recipient: String, epoch: Long): FStream[IO, Msg]
  // output
  protected def out(recipient: String, msgs: Seq[Msg]): FStream[IO, E]

  // adds 'msg' to db and checks if it's epoch matches with current epoch
  def consistentAdd(recipient: String, msg: Msg, lastState: BufferState, prevState: Option[BufferState] = None): IO[Long] = {
    async[IO] {
      prevState.fold(addMessage(recipient, msg.copy(epoch = lastState.epoch))) { ps =>
        changeMessageEpoch(recipient, ps.epoch, msg.timestamp, msg.key, msg.msg, lastState.epoch)
      }.await

      val st = readBufferState(recipient).await

      if (st.epoch == lastState.epoch) lastState.epoch
      else consistentAdd(recipient, msg, st, Some(lastState)).await
    }
  }

  def send(recipient: String, key: String, timestamp: Long, msg: M): FStream[IO, E] = {
    val io = async[IO] {
      val state = readBufferState(recipient).await
      val lastEpoch = consistentAdd(recipient, Msg(state.epoch, key, timestamp, msg), state).await
      // sleep for timeout to guarantee "timeout" period between epochs
      IO.sleep(timeout).await
      val casRes = casBufferState(recipient, State(lastEpoch), State(lastEpoch + 1)).await
      // if we are "lucky", we are the first who moved epoch, so we should
      // gather all epoch messages and move them to "out";
      // otherwise do nothing
      if (casRes) readMessages(recipient, lastEpoch).compile.fold(IndexedSeq.empty[Msg])(_ :+ _).await
      else IndexedSeq.empty[Msg]
    }

    FStream.eval(io).flatMap { seq =>
      out(recipient, seq)
    }
  }
}

object BufferLogic {
  case class State(epoch: Long)

  case class Msg[M](epoch: Long, key: String, timestamp: Long, msg: M)
}