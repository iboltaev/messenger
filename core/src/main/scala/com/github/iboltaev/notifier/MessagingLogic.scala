package com.github.iboltaev.notifier

import cats.effect.IO
import com.github.iboltaev.notifier.MessagingLogic._
import fs2.{Stream => FStream}

trait MessagingLogic[A, M] {
  type State = MessagingLogic.State[A]
  type MsgData = MessagingLogic.MsgData[M]
  val algo = MessagingLogic.Algo.apply[M] _
  val msg = MessagingLogic.Msg.apply[M] _

  // sends message to all addresses, return failed addresses
  protected def sendToAll(addresses: Set[A], epoch: Long, message: M): IO[Set[A]]
  // state logic
  protected def readState(recipient: String): IO[State]
  protected def casState(recipient: String, expect: State, newState: State): IO[Boolean]
  // log logic
  protected def readMessages(recipient: String, fromEpoch: Long, fromTS: Long, untilEpoch: Long): FStream[IO, M]
  protected def writeMessage(key: String, message: M): IO[Long] // returns timestamp
  protected def logMessage(recipient: String, epoch: Long, timestamp: Long, key: String): IO[Unit]

  private def sendMsgs(
                        msgData: MsgData,
                        lastState: State,
                        sent: Set[A] = Set.empty,
                        fails: Set[A] = Set.empty): FStream[IO, Algo[M]] =
  {
    val sendTo = lastState.addresses -- sent -- fails
    val MsgData(recipient, key, message, timestamp) = msgData

    FStream.eval(logMessage(recipient, lastState.epoch, timestamp, key)) >>
    FStream(algo(lastState.epoch, -1, s"start send to ${sendTo.size}, sent=${sent.size}, last failures=${fails.size}")) ++
    FStream.eval(sendToAll(sendTo, lastState.epoch, message)).flatMap { failures =>
      if (failures.isEmpty && fails.isEmpty) {
        // no failures and no previous fails - just check epoch
        // if epoch don't match - re-send
        FStream.eval(readState(recipient)).flatMap { state =>
          if (lastState.epoch == state.epoch)
            FStream(algo(lastState.epoch, state.epoch, "OK"))
          else
            FStream(algo(lastState.epoch, state.epoch, s"retry, no failures")) ++
            sendMsgs(msgData, state, sendTo)
        }
      } else {
        val allFails = failures ++ fails
        // failures or previous fails, changes addresses set => changes epoch
        val newState = State(lastState.epoch + 1, lastState.addresses -- allFails)
        FStream.eval(casState(recipient, lastState, newState)).flatMap { result =>
          if (result)
            FStream(algo(lastState.epoch, newState.epoch, s"failures=${failures.size}, epoch change OK")) ++
            sendMsgs(msgData, newState, sendTo ++ sent -- failures)
          else
            FStream(algo(lastState.epoch, -1, s"failures=${failures.size}, sendTo=${sendTo.size}, sent=${sent.size}, lastFails=${fails.size} epoch change failed, retry")) ++
            sendKey(msgData, sendTo ++ sent -- failures, allFails)
        }
      }
    }
  }

  private def sendKey(msgData: MsgData, sent: Set[A] = Set.empty, fails: Set[A] = Set.empty): FStream[IO, Algo[M]] = {
    val MsgData(recipient, key, message, timestamp) = msgData
    FStream.eval(readState(recipient)).flatMap { state =>
      FStream(algo(-1, state.epoch, s"start send")) ++
      sendMsgs(msgData, state, sent, fails)
    }
  }

  def send(recipient: String, key: String, message: M): FStream[IO, Algo[M]] = {
    FStream(algo(-1, -1, s"start write message key=$key")) ++
    FStream.eval(writeMessage(key, message)).flatMap { timestamp =>
      sendKey(MsgData(recipient, key, message, timestamp))
    }
  }

  private def receiveCurrent(recipient: String, addresses: Set[A], lastState: State): FStream[IO, Elem[M]] = {
    FStream(algo(lastState.epoch, -1, s"set ${addresses.size} addresses")) ++
    FStream.eval(casState(recipient, lastState, State(lastState.epoch + 1, lastState.addresses ++ addresses))).flatMap { result =>
      if (result) {
        FStream(algo(lastState.epoch, lastState.epoch + 1, s"cas OK")) ++
        readMessages(recipient, lastState.epoch, 0L, lastState.epoch + 1).map(m => msg(lastState.epoch, m))
      } else {
        FStream(algo(lastState.epoch, -1, s"cas failed")) ++
        receive(recipient, addresses, lastState.epoch, 0L)
      }
    }
  }

  def receive(recipient: String, addresses: Set[A], fromEpoch: Long, fromTS: Long): FStream[IO, Elem[M]] = {
    FStream.eval(readState(recipient)).flatMap { state =>
      FStream(algo(-1, state.epoch, s"start read fromEpoch=$fromEpoch epoch=${state.epoch}")) ++
      {
        if (fromEpoch == state.epoch) receiveCurrent(recipient, addresses, state)
        else {
          readMessages(recipient, fromEpoch, fromTS, state.epoch).map(m => msg(state.epoch, m)) ++
          receive(recipient, addresses, state.epoch, 0)
        }
      }
    }
  }
}

object MessagingLogic {
  case class State[A](epoch: Long, addresses: Set[A])

  case class MsgData[M](recipient: String, key: String, message: M, timeStamp: Long)

  sealed trait Elem[M]
  case class Msg[M](epoch: Long, message: M) extends Elem[M]
  case class Algo[M](lastEpoch: Long, epoch: Long, msg: String) extends Elem[M]
}
