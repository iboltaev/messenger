package com.github.iboltaev.messenger.client

import com.github.iboltaev.messenger.client.storage.Storage
import com.github.iboltaev.messenger.client.storage.cartesian.{CartesianMap, KVStore}
import com.github.iboltaev.messenger.client.storage.cartesian.CartesianMap.JsonMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import upickle.default._

import scala.collection.mutable

class TableSpec extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks {
  lazy val jsonMapper = new JsonMapper {
    override def jsonParse(s:  String): CartesianMap.MapRoot = read[CartesianMap.MapRoot](s)
    override def jsonStringify(mr:  CartesianMap.MapRoot): String = write(mr)
  }

  // TODO: remove copy-paste, make fixture
  def mkStore = new KVStore {
    val map = new mutable.HashMap[String, String]()
    var id = 0L

    override def getItem(k: String): String = map.getOrElse(k, null)
    override def setItem(k: String, v: String): Unit = map.update(k, v)
    override def removeItem(k: String): Unit = map.remove(k)

    override val counter: Long = id
    override def getAndInc: Long = {
      val res = id
      id += 1
      id
    }

    override def restoreCounter(value: Long): Unit = {
      id = value
    }
  }

  it should "work" in {
    val kvStore = mkStore
    val storage = Storage.storage(kvStore, jsonMapper)

    storage.tables.toSeq should be (empty)

    val table = storage.table("test")

    storage.tables.toSeq should contain theSameElementsAs(Seq("test"))

    table.upsert("u111", "o111", "value-1")
    table.iterator.toSeq should contain theSameElementsAs(Seq(("o111", "u111", "value-1")))
  }
}
