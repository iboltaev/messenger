package com.github.iboltaev.notifier

import cats.effect.IO
import com.github.iboltaev.notifier.BatchMessagingLogic._
import fs2.{Stream => FStream}

trait BatchMessagingLogic[A, M] {
  type State = BatchMessagingLogic.State[A]
  type MsgData = BatchMessagingLogic.MsgData[M]
  val algo = BatchMessagingLogic.Algo.apply[M] _
  val msg = BatchMessagingLogic.Msg.apply[M] _

  // sends message to all addresses, return failed addresses
  protected def sendToAll(recipient: String, addresses: Set[A], epoch: Long, messages: Map[String, FullMessage[M]]): IO[Set[A]]
  // state logic
  protected def readState(recipient: String): IO[State]
  protected def casState(recipient: String, expect: State, newState: State): IO[Boolean]
  // log logic
  protected def readMessages(recipient: String, fromEpoch: Long, fromTS: Long, untilEpoch: Long): FStream[IO, M]
  protected def writeMessages(messages: Map[String, M]): IO[Long] // returns timestamp
  protected def logMessages(recipient: String, epoch: Long, timestamp: Long, keys: Set[String]): IO[Unit]

  // TODO: refactor
  private def sendMsgs(
                        msgData: MsgData,
                        newAddresses: Set[A],
                        lastState: State,
                        sent: Set[A] = Set.empty,
                        fails: Set[A] = Set.empty): FStream[IO, Algo[M]] =
  {
    val allAddrs = lastState.addresses ++ newAddresses -- fails
    val sendTo = allAddrs -- sent
    val MsgData(recipient, messages, timestamp) = msgData

    // we log messages only when we add all 'newAddresses', because
    // if we have 'newAddresses' to add, we are guaranteed that there
    // will be new epoch, and we add messages to log only then
    val log =
      if (newAddresses.isEmpty)
        FStream.eval(logMessages(recipient, lastState.epoch, timestamp, messages.keySet))
      else FStream(())

    log >>
    FStream(algo(lastState.epoch, -1, s"start send to ${sendTo.size}, sent=${sent.size}, last failures=${fails.size}")) ++
    FStream.eval(sendToAll(recipient, sendTo, lastState.epoch, messages.map { case (id, m) =>
      id -> FullMessage(id, lastState.epoch, timestamp, m)
    })).flatMap { failures =>
      // it's important here that only if we have no new addresses,
      // no current failures and no previous failures,
      // we can go without epoch increment
      if (newAddresses.isEmpty && failures.isEmpty && fails.isEmpty) {
        // no failures and no previous fails - just check epoch
        // if epoch don't match - re-send
        FStream.eval(readState(recipient)).flatMap { state =>
          if (lastState.epoch == state.epoch)
            FStream(algo(lastState.epoch, state.epoch, "OK"))
          else
            FStream(algo(lastState.epoch, state.epoch, s"retry, no failures")) ++
            sendMsgs(msgData, newAddresses, state, allAddrs)
        }
      } else {
        // we have failures, or prev fails, or new addresses -
        // in any case, there is address set change, so - epoch movement
        val allFails = failures ++ fails
        val newAllAddrs = allAddrs -- failures
        // failures or previous fails, changes addresses set => changes epoch
        val newState = State(lastState.epoch + 1, newAllAddrs)
        FStream.eval(casState(recipient, lastState, newState)).flatMap { result =>
          if (result)
            FStream(algo(lastState.epoch, newState.epoch, s"failures=${failures.size}, epoch change OK")) ++
              sendMsgs(msgData, Set.empty, newState, newAllAddrs)
          else
            FStream(algo(lastState.epoch, -1, s"failures=${failures.size}, sendTo=${sendTo.size}, sent=${sent.size}, lastFails=${fails.size} epoch change failed, retry")) ++
              sendKeys(msgData, newAddresses, newAllAddrs, allFails)
        }
      }
    }
  }

  def sendKeys(msgData: MsgData, newAddresses: Set[A], sent: Set[A] = Set.empty, fails: Set[A] = Set.empty): FStream[IO, Algo[M]] = {
    FStream.eval(readState(msgData.recipient)).flatMap { state =>
      FStream(algo(-1, state.epoch, s"start send")) ++
      sendMsgs(msgData, newAddresses, state, sent, fails)
    }
  }

  def send(recipient: String, messages: Map[String, M], newAddresses: Set[A]): FStream[IO, Algo[M]] = {
    FStream(algo(-1, -1, s"start write ${messages.size} messages")) ++
    FStream.eval(writeMessages(messages)).flatMap { timestamp =>
      sendKeys(MsgData(recipient, messages, timestamp), newAddresses)
    }
  }

  def initState(recipient: String): IO[Unit] = IO(())
}

object BatchMessagingLogic {
  case class State[A](epoch: Long, addresses: Set[A])

  case class MsgData[M](recipient: String, messages: Map[String, M], timeStamp: Long)

  sealed trait Elem[M]
  case class Msg[M](epoch: Long, message: M) extends Elem[M]
  case class Algo[M](lastEpoch: Long, epoch: Long, msg: String) extends Elem[M]

  case class FullMessage[M](id: String, epoch: Long, timestamp: Long, msg: M)
}

