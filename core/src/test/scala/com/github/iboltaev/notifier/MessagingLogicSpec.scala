package com.github.iboltaev.notifier

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.MessagingLogic._
import fs2.{Stream => FStream}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, TreeMap}

trait DeliveryMock {
  def isDeliveryFailed(id: Int): Boolean
  var deliverCounter = 0
  val delivered = new ArrayBuffer[Elem[String]]()

  protected def sendToAll(addresses: Set[String], epoch: Long, message: String): IO[Set[String]] = IO {
    val res = !isDeliveryFailed(deliverCounter)
    if(res) delivered += Msg[String](epoch, message)
    deliverCounter += 1

    if (res) Set.empty
    else addresses.headOption.toSet
  }
}

trait StateMock {
  var stateCounter = 0
  var state: State[String] = State(epoch = 0L, addresses = Set.empty)
  def changeState(old: State[String]): State[String]

  protected def readState(recipient: String): IO[State[String]] =
    IO {
      state = changeState(state)
      stateCounter += 1
      state
    }

  protected def casState(recipient: String, expected: State[String], newState: State[String]): IO[Boolean] =
    IO {
      val newSt = changeState(state)
      stateCounter += 1
      val result = newSt == expected
      if (result) state = newState else state = newSt
      result
    }
}

trait MessagesMock {
  var messagesCount = 0
  val messages = new mutable.HashMap[String, String]()
  val messageLog = new TreeMap[(Long, Long), ArrayBuffer[String]]()

  def readMessages(recipient: String, fromEpoch: Long, fromTS: Long, untilEpoch: Long): fs2.Stream[IO, String] = {
    lazy val it = for {
      epoch <- messageLog.iteratorFrom((fromEpoch, fromTS)).takeWhile(p => p._1._1 < untilEpoch)
      key <- epoch._2.iterator
    } yield messages(key)

    FStream.fromIterator[IO].apply(it, chunkSize = 1)
  }

  def logMessage(recipient: String, epoch: Long, timeStamp: Long, key: String): IO[Unit] =
    IO {
      messageLog.getOrElseUpdate((epoch, timeStamp), new ArrayBuffer[String]()).append(key)
    }

  def writeMessage(key: String, msg: String): IO[Long] =
    IO {
      val timeStamp = messagesCount
      messagesCount += 1
      messages.put(key, msg)
      timeStamp
    }
}

class MessagingLogicSpec extends AnyFlatSpec with Matchers {
  type StateChange = State[String] => State[String]

  val ident: StateChange = identity
  def incEpoch(inc: Int): StateChange = s => State(s.epoch + inc, s.addresses)
  val plusEpoch: StateChange = incEpoch(1)
  def trues: Iterator[Boolean] = Iterator.continually(true)
  def falses: Iterator[Boolean] = Iterator.continually(false)
  def idents: Iterator[StateChange] = Iterator.continually(ident)

  implicit lazy val runtime = IORuntime.builder().build()

  def mkLogic(
               deliveries: Iterator[Boolean] = trues,
               states: Iterator[StateChange] = idents
             ) = new MessagingLogic[String, String]
    with DeliveryMock
    with StateMock
    with MessagesMock
    {
      val deliveriesInf = deliveries ++ trues
      val statesInf = states ++ idents

      override def isDeliveryFailed(id: Int): Boolean = !deliveriesInf.next()
      override def changeState(old: MessagingLogic.State[String]): MessagingLogic.State[String] = statesInf.next()(old)

      def sendMsgFold(recipient: String, key: String, msg: String): IndexedSeq[Algo[String]] = {
        send(recipient, key, msg).compile.fold(IndexedSeq.empty[Algo[String]])(_ :+ _).unsafeRunSync()
      }

      def receiveFold(recipient: String, addresses: Set[String], fromEpoch: Long, fromTS: Long): IndexedSeq[Elem[String]] = {
        receive(recipient, addresses, fromEpoch, fromTS).compile.fold(IndexedSeq.empty[Elem[String]])(_ :+ _).unsafeRunSync()
      }
    }

  it should "run simplest case" in {
    val logic = mkLogic()

    val sendLog = logic.sendMsgFold("a", "key", "hello-1")
    println(sendLog)

    logic.state.epoch should be (0)

    val receiveLog = logic.receiveFold("a", Set("adr1"), 0L, 0L)
    println(receiveLog)

    logic.state.epoch should be (1)

    receiveLog.collect {
      case Msg(_, message) => message
    } should contain ("hello-1")
  }

  it should "send with failures" in {
    val logic = mkLogic(falses)

    val receiveLog = logic.receiveFold("a", Set("addr1"), 0, 0)
    println(receiveLog)

    logic.state.epoch should be (1)
    logic.state.addresses should contain ("addr1")

    val sendLog = logic.sendMsgFold("a", "a", "hello-1")
    println(sendLog)

    logic.state.epoch should be (2)
    logic.state.addresses should be (empty)
  }

  it should "send with retry if epoch changed" in {
    val logic = mkLogic(trues, Iterator(ident, plusEpoch, plusEpoch, plusEpoch, ident))

    val sendLog = logic.sendMsgFold("a", "key", "hello-1")
    println(sendLog)

    logic.state.epoch should be (3)
    logic.messageLog.keySet should contain theSameElementsAs Seq((0, 0), (1, 0), (2, 0), (3, 0))
  }

  it should "send & receive with contention" in {
    val logic = mkLogic(falses, Iterator(ident, plusEpoch, plusEpoch, ident, ident, plusEpoch, plusEpoch, plusEpoch, plusEpoch, ident))

    val receiveLog = logic.receiveFold("a", Set("addr1", "addr2", "addr3"), 0, 0)
    println(receiveLog)

    val sendLog = logic.sendMsgFold("a", "key", "hello-1")
    println(sendLog)

    logic.state.epoch should be (8)
    logic.state.addresses should contain theSameElementsAs Seq("addr2", "addr3")
    logic.messageLog.keySet should contain theSameElementsAs Seq((4, 0), (6,0), (7,0), (8,0))
  }
}
