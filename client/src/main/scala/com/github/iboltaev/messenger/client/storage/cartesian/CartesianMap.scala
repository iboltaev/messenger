package com.github.iboltaev.messenger.client.storage.cartesian

import scala.scalajs.js
import scala.scalajs.js.JSON

trait CartesianMap {
  import CartesianMap._

  def name: String
  def jsonStringify(mr: MapRoot): String

  implicit val storage: KVStore

  // impl

  var root: Option[CartesianTree] = None

  def getRoot: CartesianTree = {
    if (root.nonEmpty) root.get
    else readRoot // sets root
  }

  private def readRoot = {
    val opt = Option(storage.getItem(name))
    opt.fold(CartesianTree.empty) { s =>
      val mr = JSON.parse(s).asInstanceOf[MapRoot]
      mr.toAdd.foreach(p => CartesianTree.write(p._2))
      mr.toRemove.foreach(CartesianTree.del)
      val result = CartesianTree.read(mr.rootId, Map.empty)
      root = Some(result)
      result
    }
  }

  def setRoot(merge: CartesianTree.Merge) = {
    val mr = new MapRoot(merge.tree.id, merge.toAdd, merge.toRemove)
    val str = jsonStringify(mr)
    storage.setItem(name, str) // first phase
    mr.toAdd.foreach(p => CartesianTree.write(p._2))
    mr.toRemove.foreach(CartesianTree.del)
    val mr2 = new MapRoot(rootId = mr.rootId, toAdd = Map.empty, toRemove = Seq.empty)
    val str2 = jsonStringify(mr2)
    storage.setItem(name, str2) // second phase
    root = Some(merge.tree)
  }

  def upsertLogic(key: String, value: String) = {
    val tree = getRoot
    val spl = CartesianTree.split(tree, key)
    val newNode = CartesianTree.newTree(key, value)
    val mr1 = CartesianTree.merge(spl.left, newNode, spl.toAdd.updated(newNode.id, newNode), spl.toRemove)
    val res = CartesianTree.merge(mr1.tree, spl.right, mr1.toAdd, mr1.toRemove)
    res
  }

  def upsert(key: String, value: String) = {
    setRoot(upsertLogic(key, value))
  }

  def removeLogic(key: String) = {
    val tree = getRoot
    val spl = CartesianTree.split(tree, key)
    CartesianTree.merge(spl.left, spl.right, spl.toAdd, tree.id +: spl.toRemove)
  }

  def remove(key: String) = {
    setRoot(removeLogic(key))
  }

  def iterator = getRoot.iterator.map(ct => (ct.key, ct.value))
}

object CartesianMap {
  // must not be case class, because js.Object prohibits it
  class MapRoot(val rootId: String, val toAdd: Map[String, CartesianTree], val toRemove: Seq[String]) extends js.Object
  {
    def getRootId: String = rootId
    def getToAdd: Map[String, CartesianTree] = toAdd
    def getToRemove: Seq[String] = toRemove
  }
}