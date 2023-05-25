package com.github.iboltaev.notifier.backend

import cats.effect.{IO, Resource}
import org.apache.hadoop.hbase.util.Bytes

import java.io.Closeable
import java.util.concurrent.CompletableFuture

package object hbase {
  implicit def fromJavaFuture[A](cf: => CompletableFuture[A]): IO[A] =
    IO.fromCompletableFuture(IO(cf))

  implicit def toBytes(s: String): Array[Byte] = Bytes.toBytes(s)

  implicit def mkRes[A <: Closeable](cf: => CompletableFuture[A]): Resource[IO, A] =
    Resource.make[IO, A](fromJavaFuture(cf))(c => IO(c.close()))
}
