package io.amient.affinity.core.http

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

case class RequestException(code: StatusCode) extends RuntimeException(code.intValue + " - " + code.reason) {
  def this(code: Int, reason: String, clientMessage: String) = this(StatusCodes.custom(
    code, reason, defaultMessage = clientMessage, isSuccess = false, allowsEntity = clientMessage != null
  ))
}
