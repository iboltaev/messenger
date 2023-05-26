package com.github.iboltaev.notifier.backend

import cats.effect._
import cats.effect.cps._
import cats.effect.unsafe.IORuntime
import cats.implicits._
import com.github.iboltaev.notifier.BufferLogic.State
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.strValueCodec
import com.github.iboltaev.notifier.backend.hbase.{HBaseBuffer, fromJavaFuture}
import fs2.{Stream => FStream}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{AsyncConnection, ConnectionFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZoneOffset
import scala.concurrent.duration.Duration

// TODO: make normal intergration tests
class HBaseBufferSpec extends AnyFlatSpec with Matchers {
  implicit val runtime = IORuntime.global

  def mkBuf = {
    val config = HBaseConfiguration.create()
    config.addResource("/hbase-site.xml")

    for {
      cn <- fromJavaFuture(ConnectionFactory.createAsyncConnection(config))
      buf = new HBaseBuffer[String, String] {
        override implicit val runtime: IORuntime = IORuntime.global
        override def connection: AsyncConnection = cn
        override protected def bufferStateTableName: String = "state"
        override protected def bufferMessagesTableName: String = "buffer"

        override val mValCodec: Codecs.ValueCodec[String] = strValueCodec

        override protected def timeout: Duration = Duration(1, "s")

        override protected def out(recipient: String, msgs: Seq[Msg]): FStream[IO, String] = {
          FStream(msgs.map(_.msg): _*)
        }

        def initState(recipient: String) = put[BufferStateKey, BufferState](
          bufferStateTableName, bufferStateColFamily, BufferStateKey(recipient + "-buf"), State(0))
      }
    } yield buf
  }

  it should "work" in {
    val io = async[IO] {
      val buf = mkBuf.await

      //buf.initState("ilyxa").await

      val fibers = (0 until 100).toList.map { i =>
        IO.asyncForIO.start(
          buf.send("ilyxa", i.toString, java.time.LocalDateTime.now().toEpochSecond(ZoneOffset.UTC), s"message-$i")
            .compile.toVector
        )
      }.sequence.await

      val outcomes = fibers.map(_.join).sequence.await

      println(outcomes)
    }

    io.unsafeRunSync()
  }
}
