package io.amient.affinity.core.http

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

case class RequestException(code: StatusCode, serverMessage: String) extends RuntimeException(code.intValue + " - " + code.reason + " - " + serverMessage) {

  def this(code: Int, clientReason: String, serverMessage: String) = this(
    StatusCodes.custom(code, clientReason, null, isSuccess = false, allowsEntity = false)
    , serverMessage)

}
