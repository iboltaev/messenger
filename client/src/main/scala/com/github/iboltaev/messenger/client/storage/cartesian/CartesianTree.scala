package com.github.iboltaev.messenger.client.storage.cartesian

import upickle.default._

import scala.util.Random

case class CartesianTree(id: String, key: String, left: String, right: String, size: Int, value: String, p: Int = Random.nextInt())
{
  import CartesianTree._

  def find(k: String)(implicit storage: KVStore): Option[String] = {
    if (id.isEmpty) None
    else if (k == key) Some(value)
    else if (k < key) read(left, Map.empty).find(k)
    else read(right, Map.empty).find(k)
  }

  def iterator(implicit storage: KVStore): Iterator[CartesianTree] = {
    if (id.isEmpty) Iterator.empty
    else {
      read(left, Map.empty).iterator ++ Iterator(this) ++ read(right, Map.empty).iterator
    }
  }

  def iteratorFromKey(key: String)(implicit storage: KVStore): Iterator[CartesianTree] = {
    if (id.isEmpty) Iterator.empty
    else {
      if (key < this.key)
        read(left, Map.empty).iteratorFromKey(key) ++ Iterator(this) ++ read(right, Map.empty).iterator
      else if (key == this.key)
        Iterator(this) ++ read(right, Map.empty).iterator
      else
        read(right, Map.empty).iteratorFromKey(key)
    }
  }

  def iteratorFromIdx(idx: Int)(implicit storage: KVStore): Iterator[CartesianTree] = {
    lazy val l = read(left, Map.empty)

    if (id.isEmpty) Iterator.empty
    else if (idx < l.size) l.iteratorFromIdx(idx) ++ Iterator(this) ++ read(right, Map.empty).iterator
    else if (idx == l.size) Iterator(this) ++ read(right, Map.empty).iterator
    else read(right, Map.empty).iteratorFromIdx(idx - l.size - 1)
  }

  def keyAtIdx(idx: Int)(implicit store: KVStore): Option[String] = {
    lazy val l = read(left, Map.empty)

    if (id.isEmpty) None
    else if (idx < l.size) l.keyAtIdx(idx)
    else if (idx == l.size) Some(key)
    else read(right, Map.empty).keyAtIdx(idx - l.size - 1)
  }
}

object CartesianTree {
  case class Split(left: CartesianTree, right: CartesianTree, toAdd: Map[String, CartesianTree], toRemove: Seq[String])
  case class Merge(tree: CartesianTree, toAdd: Map[String, CartesianTree], toRemove: Seq[String], toAddRaw: Map[String, String] = Map.empty)

  def empty = CartesianTree("", "", "", "", 0, "")
  def newTree(key: String, value: String)(implicit storage: KVStore) = CartesianTree(storage.getAndInc.toString, key, "", "", 1, value)

  // better to keep 'news' without default value to track usage
  def read(id: String, news: Map[String, CartesianTree])(implicit storage: KVStore): CartesianTree = {
    Option(storage.getItem(id)).map { s =>
      val arr = s.split('\u0000')
      CartesianTree(id, arr(0), arr(1), arr(2), arr(4).toInt, arr.drop(5).mkString, arr(3).toInt)
    }.orElse(news.get(id)).getOrElse(empty)
  }

  def write(ct: CartesianTree)(implicit storage: KVStore) = {
    if (ct.id.nonEmpty) storage.setItem(ct.id, s"${ct.key}\u0000${ct.left}\u0000${ct.right}\u0000${ct.p}\u0000${ct.size}\u0000${ct.value}")
  }

  def del(id: String)(implicit storage: KVStore) = {
    storage.removeItem(id)
  }

  def split(tree: CartesianTree, key: String, toAdd: Map[String, CartesianTree] = Map.empty, toRemove: Seq[String] = Seq.empty)(implicit storage: KVStore): Split = {
    if (tree.id.isEmpty) Split(empty, empty, toAdd, toRemove)
    else if (tree.key == key) Split(read(tree.left, toAdd), read(tree.right, toAdd), toAdd, tree.id +: toRemove)
    else if (key < tree.key) {
      val l = read(tree.left, toAdd)
      val spl = split(l, key, toAdd, toRemove)
      val newSize = read(tree.right, spl.toAdd).size + spl.right.size + 1
      val newRoot = CartesianTree.newTree(tree.key, tree.value).copy(left = spl.right.id, right = tree.right, p = tree.p, size = newSize)
      Split(spl.left, newRoot, spl.toAdd.updated(newRoot.id, newRoot), tree.id +: spl.toRemove)
    } else {
      val r = read(tree.right, toAdd)
      val spl = split(r, key, toAdd, toRemove)
      val newSize = read(tree.left, spl.toAdd).size + spl.left.size + 1
      val newRoot = CartesianTree.newTree(tree.key, tree.value).copy(left = tree.left, right = spl.left.id, p = tree.p, size = newSize)
      Split(newRoot, spl.right, spl.toAdd.updated(newRoot.id, newRoot), tree.id +: spl.toRemove)
    }
  }

  def merge(left: CartesianTree, right: CartesianTree, toAdd: Map[String, CartesianTree] = Map.empty, toRemove: Seq[String] = Seq.empty)(implicit storage: KVStore): Merge = {
    if (left.id.isEmpty) Merge(right, toAdd, toRemove)
    else if (right.id.isEmpty) Merge(left, toAdd, toRemove)
    else if (left.p > right.p) {
      val l = read(left.left, toAdd)
      val r1 = read(left.right, toAdd)
      val nr = merge(r1, right, toAdd, toRemove)
      val nroot = left.copy(id = storage.getAndInc.toString, left = l.id, right = nr.tree.id, size = l.size + nr.tree.size + 1)
      Merge(nroot, nr.toAdd.updated(nroot.id, nroot), left.id +: nr.toRemove)
    } else {
      val l = read(right.left, toAdd)
      val r = read(right.right, toAdd)
      val nr = merge(left, l, toAdd, toRemove)
      val nroot = right.copy(id = storage.getAndInc.toString, left = nr.tree.id, right = r.id, size = nr.tree.size + r.size + 1)
      Merge(nroot, nr.toAdd.updated(nroot.id, nroot), right.id +: nr.toRemove)
    }
  }

  implicit val rw: ReadWriter[CartesianTree] = macroRW
}
