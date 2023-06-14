package com.github.iboltaev.notifier.backend

import com.github.iboltaev.messenger.requests.Requests
import com.github.iboltaev.messenger.requests.Requests.SendMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JsonSpec extends AnyFlatSpec with Matchers {

  it should "work" in {
    val sendMessage = SendMessage("room", "message")
    val str = Requests.serialize(sendMessage)
    println(str)

    val req = Requests.parse(str)
    println(req)
  }
}
