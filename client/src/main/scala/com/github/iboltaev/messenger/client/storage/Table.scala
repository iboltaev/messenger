package com.github.iboltaev.messenger.client.storage

import com.github.iboltaev.messenger.client.storage.cartesian.KVStore

trait Table {
  def name: String

  def readUniq(uniqKey: String): Option[String]
  def upsert(uniqKey: String, orderingKey: String, value: String): Unit

  implicit val storage: KVStore
}
