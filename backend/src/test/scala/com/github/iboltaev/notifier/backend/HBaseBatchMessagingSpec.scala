package com.github.iboltaev.notifier.backend

import cats.effect.IO
import cats.effect.cps._
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.BatchMessagingLogic.{FullMessage, MsgData, State}
import com.github.iboltaev.notifier.backend.hbase.HBaseBatchMessaging
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{ValueCodec, mkStrValueCodec}
import com.github.iboltaev.notifier.backend.hbase._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{AsyncConnection, ConnectionFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// TODO: make normal integration tests
class HBaseBatchMessagingSpec extends AnyFlatSpec with Matchers {
  implicit val rnt = IORuntime.global

  def mkBatchMsg = {
    val config = HBaseConfiguration.create()
    config.addResource("/hbase-site.xml")

    for {
      cn <- fromJavaFuture(ConnectionFactory.createAsyncConnection(config))
      m = new HBaseBatchMessaging[String, String] {
        override implicit val msgValCodec: ValueCodec[String] = mkStrValueCodec("msg")

        override implicit val addressesValCodec: ValueCodec[Set[String]] = new ValueCodec[Set[String]] {
          override def encodeMap(v: Set[String], param: String): Map[String, String] = Map((param -> v.mkString(",")))
          override def decodeMap(m: Map[String, String], param: String): Set[String] = m(param).split(',').filterNot(_.isEmpty).toSet
        }

        override protected def stateTableName: String = "state"
        override protected def messagesTableName: String = "messages"
        override protected def messageLogTableName: String = "log"

        override val runtime: IORuntime = rnt

        override def connection: AsyncConnection = cn

        override protected def sendToAll(recipient: String, addresses: Set[String], epoch: Long, messages: Map[String, FullMessage[String]]): IO[Set[String]] = {
          IO {
            addresses.foreach(s => println(s"send to $s"))
            Set.empty
          }
        }

        def sendKeysIO(msgData: MsgData, addresses: Set[String]) = {
          sendKeys(msgData, addresses).compile.toVector
        }

        def sendMsgsIO(recipient: String, messages: Map[String, String], newAddresses: Set[String]) = {
          send(recipient, messages, newAddresses).compile.toVector
        }
      }
    } yield m
  }

  it should "work" in {
    val io = async[IO] {
      val m = mkBatchMsg.await

      m.initState("ilyxa").await

      val msgData = MsgData[String]("ilyxa", Map("1" -> "hello-1", "2" -> "hello-2"), 100500)
      val res = m.sendKeysIO(msgData, Set("addr1", "addr2")).await
      println(res)
    }

    io.unsafeRunSync()
  }

  it should "work-2" in {
    val io = async[IO] {
      val m = mkBatchMsg.await

      m.initState("ilyxa").await

      val res = m.sendMsgsIO("ilyxa", Map("3" -> "hello-3"), Set("addr-1", "addr-2")).await
      println(res)
    }

    io.unsafeRunSync()
  }
}
