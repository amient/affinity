package io.amient.affinity.core.http

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

case class RequestException(serverMessage: String, code: StatusCode) extends RuntimeException(code.intValue + " - " + code.reason + " - " + serverMessage) {
  def this(code: Int, serverMessage: String, clientReason: String) = this(serverMessage, StatusCodes.custom(
    code, clientReason, null, isSuccess = false, allowsEntity = false
  ))
}
