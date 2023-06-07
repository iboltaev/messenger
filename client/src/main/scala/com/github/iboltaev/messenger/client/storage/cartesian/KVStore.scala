package com.github.iboltaev.messenger.client.storage.cartesian

trait KVStore {
  def getItem(k: String): String // null if empty

  def setItem(k: String, v: String): Unit

  def removeItem(k: String): Unit

  def getAndInc: Long
}
