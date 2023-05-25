package com.github.iboltaev.notifier

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.BatchMessagingLogic._
import fs2.{Stream => FStream}
import org.scalacheck._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

trait BatchDeliveryMock {
  def isDeliveryFailed(id: Int): Boolean
  var deliverCounter = 0
  val delivered = new ArrayBuffer[Elem[String]]()

  protected def sendToAll(addresses: Set[String], epoch: Long, messages: Map[String, String]): IO[Set[String]] = IO {
    val res = !isDeliveryFailed(deliverCounter)
    if(res) delivered ++= messages.map { case (_, v) =>
      Msg[String](epoch = epoch, v)
    }
    deliverCounter += 1

    if (res) Set.empty
    else addresses.headOption.toSet
  }
}

trait BatchStateMock {
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

trait BatchMessagesMock {
  var messagesCount = 0
  val messages = new mutable.HashMap[String, String]()
  val messageLog = new mutable.TreeMap[(Long, Long), ArrayBuffer[String]]()

  def readMessages(recipient: String, fromEpoch: Long, fromTS: Long, untilEpoch: Long): fs2.Stream[IO, String] = {
    lazy val it = for {
      epoch <- messageLog.iteratorFrom((fromEpoch, fromTS)).takeWhile(p => p._1._1 < untilEpoch)
      key <- epoch._2.iterator
    } yield messages(key)

    FStream.fromIterator[IO].apply(it, chunkSize = 1)
  }

  protected def writeMessages(msgs: Map[String, String]): IO[Long] = IO {
    val timeStamp = messagesCount
    messagesCount += 1
    msgs.foreach { case (k, v) =>
      messages.put(k, v)
    }
    timeStamp
  }

  protected def logMessages(recipient: String, epoch: Long, timestamp: Long, keys: Set[String]): IO[Unit] = IO {
    messageLog.getOrElseUpdate((epoch, timestamp), new ArrayBuffer[String]()).appendAll(keys)
  }
}

class BatchMessagingLogicSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  type StateChange = State[String] => State[String]

  lazy val ident: StateChange = identity
  def incEpoch(inc: Int): StateChange = s => State(s.epoch + inc, s.addresses)
  lazy val plusEpoch: StateChange = incEpoch(1)
  def trues: Iterator[Boolean] = Iterator.continually(true)
  def falses: Iterator[Boolean] = Iterator.continually(false)
  def idents: Iterator[StateChange] = Iterator.continually(ident)

  implicit lazy val runtime = IORuntime.builder().build()

  implicit def asTraversable[T](ar: java.util.ArrayList[T]): Traversable[T] =
    ar.asScala

  def mkLogic(
               deliveries: Iterator[Boolean] = trues,
               states: Iterator[StateChange] = idents
             ) = new BatchMessagingLogic[String, String]
    with BatchDeliveryMock
    with BatchStateMock
    with BatchMessagesMock
  {
    val deliveriesInf = deliveries ++ trues
    val statesInf = states ++ idents

    override def isDeliveryFailed(id: Int): Boolean = !deliveriesInf.next()
    override def changeState(old: State): State = statesInf.next()(old)

    def sendMsgFold(recipient: String, messages: Map[String, String], newAddresses: Set[String]): IndexedSeq[Algo[String]] = {
      send(recipient, messages, newAddresses).compile.fold(IndexedSeq.empty[Algo[String]])(_ :+ _).unsafeRunSync()
    }
  }

  it should "run simple case" in {
    val logic = mkLogic()

    val log = logic.sendMsgFold("a", Map("key" -> "hello-1"), Set("addr"))
    println(log)

    logic.state.epoch should be (1)
    logic.state.addresses should be (Set("addr"))
    logic.messageLog.keySet should contain theSameElementsAs Seq((1,0))
  }

  it should "run case with contention" in {
    val logic = mkLogic(falses, Iterator(ident, plusEpoch, ident, ident, plusEpoch))

    val log = logic.sendMsgFold("a", Map("key" -> "hello-1"), Set("addr1", "addr2"))
    println(log)

    logic.state.epoch should be (3)
    logic.messageLog.keySet should contain theSameElementsAs Seq((2,0), (3, 0))
  }

  it should "not move epoch if addresses don't change" in {
    val logic = mkLogic()

    val log = logic.sendMsgFold("a", Map("key" -> "hello-1"), Set.empty)
    println(log)

    logic.state.epoch should be (0)
    logic.messageLog.keySet should contain theSameElementsAs Seq((0, 0))
  }

  it should "for any history there exists a message in log in a last epoch" in {
    val gen = for {
      d <- Gen.containerOf(Gen.oneOf(Seq(true, false)))
      s <- Gen.containerOf(Gen.oneOf(Seq(ident, plusEpoch)))
    } yield (d.asScala, s.asScala)

    forAll(gen) { case (booleans, changes) =>
      val logic = mkLogic(booleans.iterator, changes.iterator)
      val hist = logic.sendMsgFold("a", Map("key" -> "hello-1"), Set("addr1", "addr2", "addr3"))

      println(hist)

      logic.state.epoch should equal (logic.messageLog.last._1._1)
    }
  }
}

