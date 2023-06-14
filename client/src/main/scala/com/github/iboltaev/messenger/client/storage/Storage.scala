package com.github.iboltaev.messenger.client.storage

import com.github.iboltaev.messenger.client.storage.cartesian.KVStore

import scala.collection.mutable

sealed trait Storage {
  implicit val storage: KVStore

  private lazy val metaTable = Table.makeTable("meta_tbl:meta")
  private lazy val tablesMap = new mutable.TreeMap[String, Table]

  def tables: Iterator[String] = metaTable.iterator.map(_._1.drop(4))

  def table(tableName: String): Table = {
    val key = s"tbl:$tableName"
    tablesMap.get(tableName).fold {
      metaTable.upsert(key, key, key)
      val t = Table.makeTable(tableName)
      tablesMap += (tableName -> t)
      t
    }(identity)
  }
}

object Storage {
  // non-thread-safe old dear java/c++-style singleton
  private var store: Storage = null

  def storage(kvStore: KVStore): Storage = {
    if (store == null) {
      val res = new Storage {
        override implicit val storage: KVStore = kvStore
      }
      store = res
      res
    } else store
  }
}
