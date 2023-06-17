package com.github.iboltaev.messenger.requests

object Requests {
  import upickle.default._

  sealed trait ReqResp {
    val room: String
  }

  // TODO: move to common subproject
  case class RoomAddRequest(room: String) extends ReqResp
  object RoomAddRequest {
    implicit lazy val rw: ReadWriter[RoomAddRequest] = macroRW
  }

  case class RoomAddResponse(room: String) extends ReqResp
  object RoomAddResponse {
    implicit lazy val rw: ReadWriter[RoomAddResponse] = macroRW
  }

  case class SendMessage(room: String, message: String) extends ReqResp
  object SendMessage {
    implicit lazy val rw: ReadWriter[SendMessage] = macroRW
  }

  case class MessageResp(id: String, epoch: Long, timestamp: Long, room: String, text: String) extends ReqResp
  object MessageResp {
    implicit lazy val rw: ReadWriter[MessageResp] = macroRW
  }

  case class GetHistory(room: String, epoch: Long, ts: Long) extends ReqResp
  object GetHistory {
    implicit lazy val rw: ReadWriter[GetHistory] = macroRW
  }

  implicit lazy val rw: ReadWriter[ReqResp] = macroRW

  def serialize(request: ReqResp) = write(request)
  def parse(str: String): ReqResp = read[ReqResp](str)
}
