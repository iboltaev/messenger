package com.github.iboltaev.notifier.backend

import cats.effect.IO
import cats.effect.cps._
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.BufferLogic
import com.github.iboltaev.notifier.BufferLogic.State
import com.github.ibolteav.notifier.backend.hbase.HBaseClient
import com.github.ibolteav.notifier.backend.hbase.bindings.Codecs._
import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// TODO: make normal intergration tests
class HBaseBindingsSpec  extends AnyFlatSpec with Matchers {

  implicit val runtime = IORuntime.builder().build()

  case class Key(recipient: String)

  implicit val keyCodec = KeyCodec.gen[Key]
  implicit val valueCodec = ValueCodec.gen[BufferLogic.State]

  "HBaseClient" should "work with get, put, del, cas" in {
    val io = async[IO] {
      val client = HBaseClient.mkClient(new Path("/hbase-site.xml")).await

      val key = Key("ilyxa")
      val state1 = State(epoch = 10)
      val state2 = State(epoch = 11)
      val state3 = State(epoch = 12)

      client.del("table2", key).await

      client.put("table2", "cf", key, state1).await

      val v = client.get[Key, State]("table2", "cf", key).await

      v should equal (state1)

      val casRes = client.cas[Key, State, State]("table2", "cf", key, state1, state2).await

      casRes should be (true)

      val casRes2 = client.cas[Key, State, State]("table2", "cf", key, state1, state3).await

      casRes2 should be (false)

      val casRes3 = client.cas[Key, State, State]("table2", "cf", key, state2, state3).await

      casRes3 should be (true)
    }

    io.unsafeRunSync()
  }

  it should "work with read" in {
    val io = async[IO] {
      val client = HBaseClient.mkClient(new Path("/hbase-site.xml")).await

      val data = (0 until 100).map { i =>
        (Key(s"ilyxa-$i"), State(epoch = i))
      }

      data.map { case (key, _) =>
        client.del("table2", key)
      }.foldLeft(IO(()))(_ >> _).await

      data.map { case (key, state) =>
        client.put("table2", "cf", key, state)
      }.foldLeft(IO(()))(_ >> _).await

      val stream = client.read[Key, State]("table2", "cf", Key("ilyxa"), Key("ilyxaA"))
      val list = stream.compile.toList.await

      list.foreach(println)
    }

    io.unsafeRunSync()
  }
}
