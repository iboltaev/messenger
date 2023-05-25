package com.github.iboltaev.notifier.backend.hbase

import cats.effect.IO
import cats.effect.std.Dispatcher
import cats.effect.unsafe.IORuntime
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{KeyCodec, ValueCodec}
import fs2.{Stream => FStream}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

trait HBaseClient {
  implicit val runtime: IORuntime
  def connection: AsyncConnection

  private def withTable[A](tableName: String)(fn: AsyncTable[AdvancedScanResultConsumer] => IO[A]): IO[A] = {
    val table = connection.getTable(TableName.valueOf(tableName))
    fn(table)
  }

  private def withTableStream[A](tableName: String)(fn: AsyncTable[AdvancedScanResultConsumer] => FStream[IO, A]): FStream[IO, A] = {
    val table = connection.getTable(TableName.valueOf(tableName))
    fn(table)
  }

  def get[K : KeyCodec, V: ValueCodec](tableName: String, colFamily: String, key: K): IO[V] = withTable(tableName) { t =>
    t.get(implicitly[KeyCodec[K]].get(key)).map { r =>
      val codec = implicitly[ValueCodec[V]]
      codec.decode(r, colFamily)
    }
  }

  // TODO: refactor!
  def read[K: KeyCodec, V: ValueCodec](tableName: String, colFamily: String, from: K, until: K): FStream[IO, (K, V)] = {
    withTableStream(tableName) { t =>
      val keyCodec = implicitly[KeyCodec[K]]
      val valueCodec = implicitly[ValueCodec[V]]

      for {
        dispatcher <- FStream.resource(Dispatcher.sequential[IO])
        queue <- FStream.eval(cats.effect.std.Queue.unbounded[IO, Option[(K, V)]])
        _ <- FStream.bracket { IO.delay {
          val exit = new AtomicBoolean(false)

          t.scan(new Scan(keyCodec.encodeBytes(from), keyCodec.encodeBytes(until)), new AdvancedScanResultConsumer {
            override def onNext(results: Array[Result], controller: AdvancedScanResultConsumer.ScanController): Unit = {
              if (exit.getAcquire) controller.terminate()

              // in unbounded queue offer always succeeds
              dispatcher.unsafeRunSync {
                queue.tryOfferN(results.toList.map { res =>
                  val v = valueCodec.decode(res, colFamily)
                  val k = keyCodec.decode(Bytes.toString(res.getRow))
                  Some((k, v))
                })
              }
            }

            // TODO: offer exception, not 'None'
            override def onError(error: Throwable): Unit = {
              dispatcher.unsafeRunSync {
                queue.offer(None)
              }
            }

            override def onComplete(): Unit = {
              dispatcher.unsafeRunSync {
                queue.offer(None)
              }
            }
          })

          exit
        }} { exit => IO {
          exit.setRelease(true)
        }}
        value <- FStream.fromQueueNoneTerminated[IO, (K, V)](queue)
      } yield value
    }
  }

  def put[K: KeyCodec, V: ValueCodec](
                                       tableName: String,
                                       colFamily: String,
                                       key: K,
                                       value: V): IO[Unit] =
    withTable(tableName) { t =>
      t.put(implicitly[ValueCodec[V]].put(key, colFamily, value)).void
    }

  def putAll[K : KeyCodec, V : ValueCodec](
                                          tableName: String,
                                          colFamily: String,
                                          values: Seq[(K, V)]): IO[Unit] =
    withTable(tableName) { t =>
      val valueCodec = implicitly[ValueCodec[V]]
      t.putAll(values.map { case (k, v) =>
        valueCodec.put(k, colFamily, v)
      }.asJava).void
    }

  def putEmpty[K: KeyCodec](
                           tableName: String,
                           colFamily: String,
                           key: K): IO[Unit] =
    withTable(tableName) { t =>
      t.put(new Put(implicitly[KeyCodec[K]].encodeBytes(key))).void
    }

  def putAllEmpty[K: KeyCodec] (
                               tableName: String,
                               colFamily: String,
                               keys: Seq[K]): IO[Unit] =
    withTable(tableName) { t =>
      val codec = implicitly[KeyCodec[K]]
      t.putAll(keys.map(k => new Put(codec.encodeBytes(k))).asJava).void
    }

  def cas[K: KeyCodec, E: ValueCodec, V: ValueCodec](
                                       tableName: String,
                                       colFamily: String,
                                       key: K,
                                       expected: E,
                                       newValue: V): IO[Boolean] =
    withTable(tableName) { t =>
      val expectedCodec = implicitly[ValueCodec[E]]
      val map = expectedCodec.encodeMap(expected)

      require(map.size == 1, s"'expected' must contain only 1 column")

      val res = t.checkAndMutate(implicitly[KeyCodec[K]].encodeBytes(key), colFamily)
        .qualifier(map.head._1).ifEquals(map.head._2)
        .thenPut(implicitly[ValueCodec[V]].put(key, colFamily, newValue))

      res.map(_.booleanValue())
    }

  def del[K: KeyCodec](tableName: String, key: K): IO[Unit] = withTable(tableName) { t =>
    t.delete(implicitly[KeyCodec[K]].del(key)).void
  }
}

object HBaseClient {
  def mkClient(hbaseSiteXmlPath: Path)(implicit catsRuntime: IORuntime): IO[HBaseClient] = {
    val config = HBaseConfiguration.create()
    config.addResource(hbaseSiteXmlPath)

    for {
      cn <- fromJavaFuture(ConnectionFactory.createAsyncConnection(config))
      client = new HBaseClient {
        override def connection: AsyncConnection = cn
        override implicit val runtime: IORuntime = catsRuntime
      }
    } yield client
  }
}
