package io.amient.affinity.core.http

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.ws.UpgradeToWebSocket

import scala.concurrent.Promise

case class WebSocketExchange(upgrade: UpgradeToWebSocket, response: Promise[HttpResponse])
