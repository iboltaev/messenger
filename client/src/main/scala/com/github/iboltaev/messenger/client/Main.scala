package com.github.iboltaev.messenger.client

import org.scalajs.dom
import org.scalajs.dom.Event

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

object Main {
  @js.native @JSImport("/javascript.svg", JSImport.Default)
  val javascriptLogo: String = js.native

  def main(args: Array[String]): Unit = {
    println("Hello Scala.js!")

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
