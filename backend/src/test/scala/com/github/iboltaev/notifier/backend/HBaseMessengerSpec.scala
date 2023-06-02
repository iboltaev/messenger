package com.github.iboltaev.notifier.backend

import cats.effect.IO
import cats.effect.cps._
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.BatchMessagingLogic.{FullMessage, State}
import com.github.iboltaev.notifier.BufferLogic
import com.github.iboltaev.notifier.backend.hbase.{HBaseMessenger, fromJavaFuture}
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{ValueCodec, mkStrValueCodec, strValueCodec}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{AsyncConnection, ConnectionFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration

// TODO: make normal intergration tests
class HBaseMessengerSpec extends AnyFlatSpec with Matchers {
  implicit val rnt = IORuntime.global

  case class Adr(adr: String)
  case class Mess(message: String)

  def mkMess = {
    val config = HBaseConfiguration.create()
    config.addResource("/hbase-site.xml")

    for {
      cn <- fromJavaFuture(ConnectionFactory.createAsyncConnection(config))
      m = new HBaseMessenger[Adr, Mess] {
        override protected def timeout: Duration = Duration(1, "s")

        override implicit val msgValCodec: Codecs.ValueCodec[Mess] = ValueCodec.gen[Mess]
        override implicit val mValCodec: Codecs.ValueCodec[Either[Adr, Mess]] = ValueCodec.gen[Either[Adr, Mess]]
        override implicit val addressesValCodec: Codecs.ValueCodec[Set[Adr]] = new ValueCodec[Set[Adr]] {
          override def encodeMap(v: Set[Adr], param: String): Map[String, String] = Map((param -> v.map(_.adr).mkString(",")))
          override def decodeMap(m: Map[String, String], param: String): Set[Adr] = m(param).split(',').filterNot(_.isEmpty).map(Adr.apply).toSet
        }

        override protected def stateTableName: String = "state"
        override protected def messagesTableName: String = "batch_messages"
        override protected def messageLogTableName: String = "log"
        override protected def bufferStateTableName: String = "state"
        override protected def bufferMessagesTableName: String = "messages"

        override val runtime: IORuntime = rnt
        override def connection: AsyncConnection = cn

        override protected def sendToAll(recipient: String, addresses: Set[Adr], epoch: Long, messages: Map[String, FullMessage[Mess]]): IO[Set[Adr]] = IO {
          for {
            a <- addresses
            m <- messages
          } {
            println(s"send ${m._2} to ${a.adr}")
          }

          Set.empty
        }
      }
    } yield m
  }

  it should "work" in {
    val io = async[IO] {
      val m = mkMess.await

      m.initState("ilyxa").await
      
      val res1 = m.bufferSend("ilyxa", "1", 100500, Left(Adr("123"))).compile.toVector.await
      println(res1)

      val res2 = m.bufferSend("ilyxa", "2", 100500, Right(Mess("hello-1"))).compile.toVector.await
      println(res2)

      val res3 = m.bufferSend("ilyxa", "3", 100500, Right(Mess("hello-2"))).compile.toVector.await
      println(res3)
    }

    io.unsafeRunSync()
  }
}
