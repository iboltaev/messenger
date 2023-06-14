package com.github.iboltaev.messenger.requests

object Requests {
  import upickle.default._

  sealed trait Request {
    val room: String
  }

  // TODO: move to common subproject
  case class RoomAddRequest(room: String) extends Request
  object RoomAddRequest {
    implicit lazy val rw: ReadWriter[RoomAddRequest] = macroRW
  }

  case class SendMessage(room: String, message: String) extends Request
  object SendMessage {
    implicit lazy val rw: ReadWriter[SendMessage] = macroRW
  }

  case class GetHistory(room: String, epoch: Long, ts: Long) extends Request
  object GetHistory {
    implicit lazy val rw: ReadWriter[GetHistory] = macroRW
  }

  implicit lazy val rw: ReadWriter[Request] = macroRW

  def serialize(request: Request) = write(request)
  def parse(str: String): Request = read[Request](str)
}
