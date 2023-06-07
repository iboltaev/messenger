package com.github.iboltaev.messenger.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.iboltaev.messenger.client.storage.cartesian.CartesianMap.MapRoot
import com.github.iboltaev.messenger.client.storage.cartesian.{CartesianMap, KVStore}
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.util.Random

class CartesianMapSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  sealed trait Action
  case class Add(key: String, value: String) extends Action
  case class Del(key: String) extends Action

  lazy val mapper = {
    val res = new ObjectMapper()
    res.registerModule(DefaultScalaModule)
    res
  }

  def mkStorage = new KVStore {
    val map = new mutable.HashMap[String, String]()
    var id = 0L

    override def getItem(k: String): String = map.getOrElse(k, null)
    override def setItem(k: String, v: String): Unit = map.update(k, v)
    override def removeItem(k: String): Unit = map.remove(k)

    override def getAndInc: Long = {
      val res = id
      id += 1
      id
    }
  }

  def mkMap(nm: String)(implicit store: KVStore) = new CartesianMap {
    override def name: String = nm
    override def jsonStringify(mr: MapRoot): String = {
      val str = mapper.writeValueAsString(mr)
      str
    }
    override implicit val storage: KVStore = store
  }

  it should "work-1" in {
    implicit val store = mkStorage

    val cmap = new CartesianMap {
      override def name: String = "map1"
      override implicit val storage: KVStore = store

      override def jsonStringify(mr: MapRoot): String = {
        val str = mapper.writeValueAsString(mr)
        str
      }
    }

    val mr1 = cmap.upsertLogic("111", "222")
    println(mr1)

    cmap.upsert("111", "222")

    cmap.iterator.toSeq should be (Seq(("111", "222")))
    //println(store.map)

    cmap.remove("111")

    cmap.iterator.toSeq should be (empty)
    //println(store.map)
  }

  it should "work-2" in {
    implicit val store = mkStorage
    val cmap = mkMap("map-1")

    cmap.upsert("111", "222")

    println(cmap.root)
    println(store.map)

    cmap.upsert("333", "444")

    println(cmap.root)
    println(store.map)

    println(cmap.iterator.toVector)
  }

  it should "work as ordinal TreeMap for any history, collect garbage for itself" in {
    val addGen = for {
      key <- Gen.alphaNumStr
      value <- Gen.asciiPrintableStr
    } yield Seq(Add(key, value))

    val delGen = Gen.alphaNumStr.map(k => Seq(Del(k)))
    val addDelGen = addGen.map(add => Seq(add.head, Del(add.head.key)))
    val ofGen = Gen.oneOf(addGen, delGen, addDelGen)
    val histGen = Gen.listOf(ofGen).map(l => Random.shuffle(l.flatten))

    forAll(histGen) { list =>
      implicit val store = mkStorage
      val cmap = mkMap("map-1")
      val tree = TreeMap.empty[String, String]

      val (cm, tm) = list.foldLeft((cmap, tree)) { (maps, a) =>
        a match {
          case Add(key, value) => ({ maps._1.upsert(key, value); maps._1 }, maps._2.updated(key, value))
          case Del(key) => ({ maps._1.remove(key); maps._1 }, maps._2 - key)
        }
      }

      val seq = cm.iterator.toSeq
      seq should contain theSameElementsInOrderAs tm.toSeq
      if (seq.size > 0) {
        store.map.size should be (seq.size + 1)
      }
    }
  }
}
