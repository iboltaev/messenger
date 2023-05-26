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

import HBaseClient._

import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

trait HBaseClient extends DBClient {
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
    t.get(mkGet(key)).map (r => decode(r, colFamily))
  }

  // TODO: refactor!
  def read[K: KeyCodec, V: ValueCodec](tableName: String, colFamily: String, from: K, until: K): FStream[IO, (K, V)] = {
    withTableStream(tableName) { t =>
      for {
        dispatcher <- FStream.resource(Dispatcher.sequential[IO])
        queue <- FStream.eval(cats.effect.std.Queue.unbounded[IO, Option[(K, V)]])
        _ <- FStream.bracket { IO.delay {
          val exit = new AtomicBoolean(false)

          t.scan(new Scan(encodeBytes(from), encodeBytes(until)), new AdvancedScanResultConsumer {
            override def onNext(results: Array[Result], controller: AdvancedScanResultConsumer.ScanController): Unit = {
              if (exit.getAcquire) controller.terminate()

              // in unbounded queue offer always succeeds
              dispatcher.unsafeRunSync {
                queue.tryOfferN(results.toList.map { res =>
                  val v = decode(res, colFamily)
                  val k = decode(Bytes.toString(res.getRow))
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
    withTable(tableName) ( t => t.put(mkPut(colFamily, key, value)).void )

  def putAll[K : KeyCodec, V : ValueCodec](
                                          tableName: String,
                                          colFamily: String,
                                          values: Seq[(K, V)]): IO[Unit] =
    withTable(tableName) { t =>
      t.putAll(values.map { case (k, v) =>
        mkPut(colFamily, k, v)
      }.asJava).void
    }

  def cas[K: KeyCodec, E: ValueCodec, V: ValueCodec](
                                       tableName: String,
                                       colFamily: String,
                                       key: K,
                                       expected: E,
                                       newValue: V): IO[Boolean] =
    withTable(tableName) { t =>
      import HBaseClient.toBytes
      val expectedCodec = implicitly[ValueCodec[E]]
      val map = expectedCodec.encodeMap(expected)

      require(map.size == 1, s"'expected' must contain only 1 column")

      val res = t.checkAndMutate(encodeBytes(key), colFamily)
        .qualifier(map.head._1).ifEquals(map.head._2)
        .thenPut(mkPut(colFamily, key, newValue))

      res.map(_.booleanValue())
    }

  def del[K: KeyCodec](tableName: String, key: K): IO[Unit] = withTable(tableName) { t =>
    t.delete(mkDel(key)).void
  }
}

object HBaseClient {
  implicit def toBytes(s: String): Array[Byte] = Bytes.toBytes(s)

  def encode[K: KeyCodec](key: K): String = {
    implicitly[KeyCodec[K]].encodeVec(key).mkString(",")
  }

  def encodeBytes[K: KeyCodec](key: K): Array[Byte] = encode(key)

  def decode[K: KeyCodec](s: String): K = {
    implicitly[KeyCodec[K]].decodeVec(s.split(',').toVector)
  }

  def decode[V: ValueCodec](res: Result, colFamily: String): V = {
    val fm = res.getFamilyMap(colFamily)
    val map = fm.asScala.map { case (bytes, bytes1) =>
      (Bytes.toString(bytes), Bytes.toString(bytes1))
    }.toMap

    implicitly[ValueCodec[V]].decodeMap(map)
  }

  def mkGet[K: KeyCodec](key: K) = new Get(encode(key))

  def mkDel[K: KeyCodec](key: K) = new Delete(encode(key))

  def mkPut[K: KeyCodec, V: ValueCodec](colFamily: String, key: K, value: V) = {
    val res = new Put(encodeBytes(key))
    val map = implicitly[ValueCodec[V]].encodeMap(value)
    map.foldLeft(res) { (p, t) =>
      p.addColumn(colFamily, t._1, t._2)
    }
  }

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

