package com.github.iboltaev.messenger.client

import com.github.iboltaev.messenger.client.storage.cartesian.CartesianMap.JsonMapper
import com.github.iboltaev.messenger.client.storage.cartesian.{CartesianMap, KVStore}
import com.github.iboltaev.messenger.client.storage.{Storage => LocalStorage}
import org.scalajs.dom
import org.scalajs.dom.{Event, Storage}

import upickle.default._

import scala.scalajs.js

import scala.scalajs.js.annotation.JSImport

object Main {
  @js.native @JSImport("/javascript.svg", JSImport.Default)
  val javascriptLogo: String = js.native

  private var dataStorage: LocalStorage = null
  private def storage(store: Storage): LocalStorage = {
    if (dataStorage != null) dataStorage
    else {
      val kvStore = new KVStore {
        private var cnt = 0L
        override def getItem(k: String): String = store.getItem(k)
        override def setItem(k: String, v: String): Unit = store.setItem(k, v)
        override def removeItem(k: String): Unit = store.removeItem(k)
        override def counter: Long = cnt
        override def getAndInc: Long = { val res = cnt; cnt += 1; res }
        override def restoreCounter(value: Long): Unit = { cnt = value }
      }

      val res = LocalStorage.storage(kvStore)
      dataStorage = res
      res
    }
  }

  def main(args: Array[String]): Unit = {
    println("Hello Scala.js!")

    val localStorage = storage(dom.window.localStorage)
    
    val table = localStorage.table("test-table")
    table.upsert("u111", "o111", "value-1")
    table.upsert("u000", "o000", "value-0")

    println(s"tables: ${localStorage.tables.toVector}")
    table.iterator.foreach(println)

    dom.document.querySelector("#app").innerHTML = s"""
    <div>
      <a href="https://vitejs.dev" target="_blank">
        <img src="/vite.svg" class="logo" alt="Vite logo" />
      </a>
      <a href="https://developer.mozilla.org/en-US/docs/Web/JavaScript" target="_blank">
        <img src="$javascriptLogo" class="logo vanilla" alt="JavaScript logo" />
      </a>
      <h1>Hello Scala.js!</h1>
      <div class="card">
        <button id="counter" type="button"></button>
      </div>
      <p class="read-the-docs">
        Click on the Vite logo to learn more
      </p>
    </div>
  """

    setupCounter(dom.document.getElementById("counter"))
  }

  def setupCounter(element: dom.Element) = {
    var counter = 0

    def setCounter(count: Int): Unit = {
      counter = count
      element.innerHTML = s"count is $counter"
    }

    element.addEventListener("click", (e: Event) => setCounter(counter + 1))
    setCounter(0)
  }
}
