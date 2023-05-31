package com.github.iboltaev.notifier.backend.hbase

import cats.effect.IO
import com.github.iboltaev.notifier.backend.hbase.bindings.Codecs.{KeyCodec, ValueCodec}
import fs2.{Stream => FStream}

// TODO: move 'colFamily' out of here, it's abstraction's leak
trait DBClient {
  def get[K : KeyCodec, V: ValueCodec](tableName: String, colFamily: String, key: K): IO[V]
  def read[K: KeyCodec, V: ValueCodec](tableName: String, colFamily: String, from: K, until: K): FStream[IO, (K, V)]
  def put[K: KeyCodec, V: ValueCodec](tableName: String, colFamily: String, key: K, value: V): IO[Unit]
  def putAll[K : KeyCodec, V : ValueCodec](tableName: String, colFamily: String, values: Seq[(K, V)]): IO[Unit]
  def del[K: KeyCodec](tableName: String, key: K): IO[Unit]

  def cas[K: KeyCodec, E: ValueCodec, V: ValueCodec](
                                                      tableName: String,
                                                      colFamily: String,
                                                      key: K,
                                                      expected: E,
                                                      newValue: V): IO[Boolean]
}
