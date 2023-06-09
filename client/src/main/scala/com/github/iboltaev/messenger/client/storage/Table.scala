package com.github.iboltaev.messenger.client.storage

import com.github.iboltaev.messenger.client.storage.cartesian.{CartesianMap, KVStore}
import upickle.default._

trait Table { self =>
  implicit val storage: KVStore
  def name: String

  lazy val cartMap = new CartesianMap {
    override def name: String = s"msg:tbl:cart:${self.name}"
    override def jsonParse(s: String): CartesianMap.MapRoot = read[CartesianMap.MapRoot](s)
    override def jsonStringify(mr: CartesianMap.MapRoot): String = write(mr)
    override implicit val storage: KVStore = self.storage
  }

  private def mapIterator(it: Iterator[(String, String)]): Iterator[(String, String, String)] = for {
    (ordKey, uniqKey) <- cartMap.iterator
    (_, value) <- readUniq(uniqKey)
  } yield (ordKey, uniqKey, value)

  def readUniq(uniqKey: String): Option[(String, String)] = // (orderingKey, value)
    Option(storage.getItem(uniqKey)).map { content =>
      val Array(ordKey, value) = content.split('\u0000')
      (ordKey, value)
    }

  def upsert(uniqKey: String, orderingKey: String, value: String): Unit = {
    val delChanges = cartMap.removeLogic(orderingKey)
    val upChanges = cartMap.upsertLogic(orderingKey, uniqKey, delChanges.toAdd, delChanges.toRemove)
    val content = s"${orderingKey}\u0000$value"
    val allChanges = upChanges.copy(toAddRaw = upChanges.toAddRaw.updated(uniqKey, content))
    cartMap.setRoot(allChanges)
  }

  def deleteUniq(uniqKey: String): Unit = {
    readUniq(uniqKey).foreach { case (ord, _) =>
      val changes = cartMap.removeLogic(ord)
      val allChanges = changes.copy(toRemove = uniqKey +: changes.toRemove)
      cartMap.setRoot(allChanges)
    }
  }

  //                      uniqKey ordKey  value
  def iterator: Iterator[(String, String, String)] = mapIterator(cartMap.iterator)
  def iteratorFromKey(key: String): Iterator[(String, String, String)] = mapIterator(cartMap.iteratorFromKey(key))

}

object Table {
  def makeTable(tableName: String)(implicit store: KVStore) = new Table {
    override def name: String = tableName
    override implicit val storage: KVStore = store
  }
}
