package com.github.iboltaev.messenger.client.storage.cartesian

import upickle.default._

trait CartesianMap {
  import CartesianMap._

  def name: String
  def jsonParse(s: String): MapRoot
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
      val mr = jsonParse(s)
      mr.toAdd.foreach(p => CartesianTree.write(p._2))
      mr.toRemove.foreach(CartesianTree.del)
      val result = CartesianTree.read(mr.rootId, Map.empty)
      root = Some(result)
      result
    }
  }

  def setRoot(merge: CartesianTree.Merge) = {
    val mr = new MapRoot(merge.tree.id, merge.toAdd, merge.toRemove, storage.counter, merge.toAddRaw)
    val str = jsonStringify(mr)
    storage.setItem(name, str) // first phase
    mr.toAddRaw.foreach(p => storage.setItem(p._1, p._2))
    mr.toAdd.foreach(p => CartesianTree.write(p._2))
    mr.toRemove.foreach(CartesianTree.del)
    val mr2 = new MapRoot(rootId = mr.rootId, toAdd = Map.empty, toRemove = Seq.empty, counter = storage.counter, toAddRaw = Map.empty)
    val str2 = jsonStringify(mr2)
    storage.setItem(name, str2) // second phase
    root = Some(merge.tree)
  }

  def upsertLogic(key: String, value: String, toAdd: Map[String, CartesianTree] = Map.empty, toRemove: Seq[String] = Seq.empty) = {
    val tree = getRoot
    val spl = CartesianTree.split(tree, key, toAdd, toRemove)
    val newNode = CartesianTree.newTree(key, value)
    val mr1 = CartesianTree.merge(spl.left, newNode, spl.toAdd.updated(newNode.id, newNode), spl.toRemove)
    val res = CartesianTree.merge(mr1.tree, spl.right, mr1.toAdd, mr1.toRemove)
    res
  }

  def find(key: String): Option[String] = getRoot.find(key)

  def upsert(key: String, value: String) = {
    setRoot(upsertLogic(key, value))
  }

  def removeLogic(key: String, toAdd: Map[String, CartesianTree] = Map.empty, toRemove: Seq[String] = Seq.empty) = {
    val tree = getRoot
    val spl = CartesianTree.split(tree, key, toAdd, toRemove)
    CartesianTree.merge(spl.left, spl.right, spl.toAdd, tree.id +: spl.toRemove)
  }

  def remove(key: String) = {
    setRoot(removeLogic(key))
  }

  def iterator = getRoot.iterator.map(ct => (ct.key, ct.value))

  def iteratorFromKey(key: String) = getRoot.iteratorFromKey(key).map(ct => (ct.key, ct.value))
  def iteratorFromIdx(idx: Int) = getRoot.iteratorFromIdx(idx).map(ct => (ct.key, ct.value))
  def keyAtIdx(idx: Int) = getRoot.keyAtIdx(idx)
}

object CartesianMap {
  // must not be case class, because js.Object prohibits it
  case class MapRoot(rootId: String, toAdd: Map[String, CartesianTree], toRemove: Seq[String], counter: Long, toAddRaw: Map[String, String])

  object MapRoot {
    implicit val rw: ReadWriter[MapRoot] = macroRW
  }

  trait JsonMapper {
    def jsonParse(s: String): CartesianMap.MapRoot
    def jsonStringify(mr: CartesianMap.MapRoot): String
  }
}