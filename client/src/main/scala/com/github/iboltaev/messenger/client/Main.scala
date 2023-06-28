package com.github.iboltaev.messenger.client

import cats.effect.cps._
import cats.effect.std.{Dispatcher, Queue}
import cats.effect.{ExitCode, IO, IOApp, Ref}
import com.github.iboltaev.messenger.client.net.Net
import com.github.iboltaev.messenger.requests.Requests
import org.scalajs.dom
import org.scalajs.dom._

import scala.collection.immutable.TreeMap

object Main extends IOApp {
  case class RoomState(
                        lastEpoch: Long = 0,
                        lastTimestamp: Long = 0,
                        msgs: TreeMap[(Long, Long), String] = TreeMap.empty)
  case class State(
                    rooms: TreeMap[String, RoomState] = TreeMap.empty,
                    activeRoom: Option[String] = None,
                    allMessages: Map[String, Requests.MessageResp] = Map.empty)

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
    val state = Ref.of[IO, State](State()).await

    val sendQueue = Queue.unbounded[IO, Option[String]].await
    Dispatcher.sequential[IO].use { dispatcher => async[IO] {
      def handleOpen(ev: Event) = {
        println("open")
      }

      def handleClose(ev: CloseEvent) = {
        println("closed")
      }

      def addRoomToScreen(room: String): Unit = {
        val newMsg = dom.document.createElement("div")
        newMsg.classList.add("room")
        newMsg.textContent = room

        newMsg.addEventListener("click", e => handleSelectRoom(e, room))

        document.getElementById("rooms").appendChild(newMsg)
      }

      def addMessageToScreen(msg: String): Unit = {
        val newMsg = dom.document.createElement("div")
        newMsg.classList.add("msg")
        newMsg.textContent = msg
        document.getElementById("messages").appendChild(newMsg)
      }

      def handleMessage(ev: MessageEvent): IO[Unit] = async[IO]{
        IO.consoleForIO.println("message: " + ev.data).await
        val obj = Requests.parse(ev.data.toString)
        obj match {
          // handle room add notification
          case Requests.RoomAddResponse(room) =>
            state.updateAndGet { old =>
              old.rooms.get(room).fold {
                old.copy(rooms = old.rooms + (room -> RoomState(0, 0, TreeMap.empty)))
              }(_ => old)
            }.void.await

            IO.delay(addRoomToScreen(room)).await

          // handle message - the main functional
          case Requests.MessageResp(id, epoch, timestamp, room, text) =>
            val newSt = state.updateAndGet { old =>
              if (old.allMessages.contains(id)) old
              else {
                val rm = old.rooms.getOrElse(room, RoomState())
                val newRoom = rm.copy(
                  lastEpoch = Math.max(rm.lastEpoch, epoch),
                  lastTimestamp = Math.max(rm.lastTimestamp, timestamp),
                  msgs = rm.msgs + ((epoch, timestamp) -> text))
                old.copy(rooms = old.rooms.updated(room, newRoom))
              }
            }.await

            if (newSt.activeRoom.contains(room)) IO.delay(addMessageToScreen(text)).await

          case _ => IO.unit
        }
      }

      def handleError(ev: ErrorEvent) = {
        println("error: " + ev)
      }

      def handleAddRoom(input: Element, e: Event): Unit = {
        val inp = input.asInstanceOf[HTMLInputElement]
        val value = inp.value
        println(s"value=$value")
        val json = Requests.serialize(Requests.RoomAddRequest(value))
        println(s"json=$json")
        dispatcher.unsafeRunAndForget(sendQueue.offer(Some(json)))
      }

      def handleSelectRoom(event: Event, room: String): Unit = {
        val msgs = dom.document.getElementById("messages")
        msgs.innerHTML = ""
        dispatcher.unsafeRunAndForget { async[IO] {
          val st = state.updateAndGet(s => s.copy(activeRoom = Some(room))).await
          st.rooms.get(room).foreach { rs =>
            rs.msgs.foreach { case ((epoch, ts), text) =>
              addMessageToScreen(text)
            }
          }
        }}
      }

      def handleEnter(input: Element, e: Event) = {
        val inp = input.asInstanceOf[HTMLInputElement]
        val kbe = e.asInstanceOf[KeyboardEvent]
        if (kbe.keyCode == 13) {
          dispatcher.unsafeRunAndForget { async[IO] {
            val st = state.get.await
            if (st.activeRoom.isEmpty) ()
            else {
              val sendMsg = Requests.serialize(
                Requests.SendMessage(st.activeRoom.get, inp.value))
              sendQueue.offer(Some(sendMsg)).await
            }
          }}
        }
      }

      IO.delay {
        dom.document.body.innerHTML =
          """
            |<div class="parent" id="container">
            | <div class="child" id="header"></div>
            | <div class="main" id="middle">
            |   <div class="child sidebar">
            |     <div>
            |       <button id="plusRoom"><b>+</b></button>
            |       <input type="text" id="inputRoom" class="inputs" value="ilyxa"></input>
            |     </div>
            |     <div id = "rooms">
            |     </div>
            |   </div>
            |   <div class="child content" id="messages">
            |
            |   </div>
            | </div>
            | <div class="child footer" id="footer">
            | <b>Enter:<b><input type="text" id="inputText" class="inputs"></input>
            | </div>
            |</div>
            |""".stripMargin

        val plusButton = dom.document.getElementById("plusRoom")
        val inputRoom = dom.document.getElementById("inputRoom")
        plusButton.addEventListener("click", e => handleAddRoom(inputRoom, e))

        val inputText = dom.document.getElementById("inputText")
        inputText.addEventListener("keydown", e => handleEnter(inputText, e))
      }.await

      val webSocketData = Net.webSocketStream("ws://localhost:8090/ws", sendQueue, async[IO] {
        // TODO: on reconnect 1) send room add request 2) request history
        IO.consoleForIO.println(s"Reconnected!").await
      })
      val fiber = webSocketData.foreach {
        case Left(value) =>
          IO.consoleForIO.println(s"Handled $value")
        case Right(value) =>
          handleMessage(value)
      }.compile.drain.start.await

      IO.consoleForIO.println("Done.").await
      fiber.join.await
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
