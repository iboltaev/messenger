package com.github.iboltaev.notifier

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.BufferLogic._
import fs2.{Stream => FStream}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.concurrent.duration.Duration


trait BufferStateMock {
  var stateCounter = 0
  var state: State = State(epoch = 0L)
  def changeState(old: State): State

  protected def readBufferState(recipient: String): IO[State] =
    IO {
      state = changeState(state)
      stateCounter += 1
      state
    }

  protected def casBufferState(recipient: String, expected: State, newState: State): IO[Boolean] =
    IO {
      val newSt = changeState(state)
      stateCounter += 1
      val result = newSt == expected
      if (result) state = newState else state = newSt
      result
    }
}

trait BufferMessagesMock {
  val messageLog = new mutable.TreeMap[(Long, Long, String), Msg[String]]

  protected def addMessage(recipient: String, msg: Msg[String]): IO[Unit] = IO {
    messageLog.update((msg.epoch, msg.timestamp, msg.key), msg)
  }

  def changeMessageEpoch(recipient: String, epoch: Long, timestamp: Long, key: String, value: String, toEpoch: Long): IO[Unit] = IO {
    messageLog.remove((epoch, timestamp, key))
    messageLog += ((toEpoch, timestamp, key) -> Msg[String](toEpoch, key, timestamp, value))
  }

  protected def readMessages(recipient: String, epoch: Long): FStream[IO, Msg[String]] = {
    val it = messageLog.iteratorFrom((epoch, 0L, ""))
    FStream.fromIterator[IO](it, chunkSize = 1).map(_._2)
  }
}

class BufferLogicSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  type StateChange = State => State

  lazy val ident: StateChange = identity
  def incEpoch(inc: Int): StateChange = s => State(s.epoch + inc)
  lazy val plusEpoch: StateChange = incEpoch(1)
  def trues: Iterator[Boolean] = Iterator.continually(true)
  def falses: Iterator[Boolean] = Iterator.continually(false)
  def idents: Iterator[StateChange] = Iterator.continually(ident)

  implicit lazy val runtime = IORuntime.builder().build()

  implicit def asTraversable[T](ar: java.util.ArrayList[T]): Traversable[T] =
    ar.asScala

  def mkLogic(deliveries: Iterator[Boolean] = trues,
              states: Iterator[StateChange] = idents) =
    new BufferLogic[String, String]
    with BufferStateMock
    with BufferMessagesMock
    {
      val deliveriesInf = deliveries ++ trues
      val statesInf = states ++ idents

      override protected def timeout: Duration = Duration.Zero

      override protected def out(recipient: String, msgs: Seq[Msg]): FStream[IO, String] = {
        FStream.fromIterator[IO](msgs.iterator, 1).map(_.msg)
      }

      override def changeState(old: BufferState): BufferState = statesInf.next()(old)

      def consistentAddSync(recipient: String, msg: Msg): Long = {
        consistentAdd(recipient, msg, State(0)).unsafeRunSync()
      }
    }

  it should "for any history there should be message in last epoch" in {
    val gen = Gen.containerOf(Gen.oneOf(Seq(ident, plusEpoch, incEpoch(2), incEpoch(3))))

    forAll(gen) { history =>
      val logic = mkLogic(trues, history.asScala.iterator)

      val epoch = logic.consistentAddSync("1", Msg(0, "key", 0, "hello-1"))

      println(s"history=${history.size}, epoch=$epoch")

      epoch should equal (logic.state.epoch)
      logic.messageLog.size should be (1)
      logic.messageLog.head._1._1 should equal (epoch)
    }
  }
}
