package com.github.iboltaev.messenger.client

import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.cps._
import cats.effect.std.{Dispatcher, Queue}
import cats.effect.syntax.all._
import com.github.iboltaev.messenger.client.net.Net
import com.github.iboltaev.messenger.client.storage.cartesian.KVStore
import com.github.iboltaev.messenger.client.storage.{Storage => LocalStorage}
import com.github.iboltaev.messenger.requests.Requests
import org.scalajs.dom
import org.scalajs.dom._

import scala.scalajs.js
import scala.scalajs.js.annotation.JSImport

object Main extends IOApp {
  /*
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

   */


  def work: IO[Unit] = async[IO] {
    val sendQueue = Queue.unbounded[IO, Option[String]].await
    Dispatcher.sequential[IO].use { dispatcher => async[IO] {
      def handleOpen(ev: Event) = {
        println("open")
      }

      def handleClose(ev: CloseEvent) = {
        println("closed")
      }

      def handleMessage(ev: MessageEvent) = {
        println("message: " + ev.data)
      }

      def handleError(ev: ErrorEvent) = {
        println("error: " + ev)
      }

      def handleAddRoom(e: Event) = {
        val json = Requests.serialize(Requests.RoomAddRequest("ilyxa"))
        println(s"json=$json")
        dispatcher.unsafeRunAndForget(sendQueue.offer(Some(json)))
      }

      IO.delay {
        dom.document.body.innerHTML =
          """
            |<div class="parent" id="container">
            | <div class="child" id="header"></div>
            | <div class="main" id="middle">
            |   <div class="child sidebar" id="rooms">
            |     <div><button id="plusRoom"><b>+</b></button></div>
            |   </div>
            |   <div class="child content" id="messages">
            |   aaaaaaaaaaabbbbbbbbbbbccccccc
            |   </div>
            | </div>
            | <div class="child footer" id="footer">
            |</div>
            |""".stripMargin

        dom.document.getElementById("plusRoom")
          .addEventListener("click", handleAddRoom)
      }.await

      val webSocketData = Net.webSocketStream("ws://localhost:8090/ws", sendQueue)
      val fiber = webSocketData.compile.drain.start.await
      IO.consoleForIO.println("Done.").await
      fiber.join.void.await
      IO.consoleForIO.println("Done - 2").await
      ()
    }}.await
  }

  override def run(args: List[String]): IO[ExitCode] = async[IO] {
    /*
        val localStorage = storage(dom.window.localStorage)

    val table = localStorage.table("test-table")
    table.upsert("u111", "o111", "value-1")
    table.upsert("u000", "o000", "value-0")
     */




    val fiber = work.start.await

    ExitCode.Success
  }
}
