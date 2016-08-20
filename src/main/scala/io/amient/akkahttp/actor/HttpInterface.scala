package io.amient.akkahttp.actor

import java.util.Properties

import akka.actor.{Actor, Props}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object HttpInterface {
  final val CONFIG_HTTP_HOST = "http.host"
  final val CONFIG_HTTP_PORT = "http.port"
}

class HttpInterface(appConfig: Properties) extends Actor {

  implicit val system = context.system

  val log = Logging.getLogger(system, this)

  import HttpInterface._

  val httoHost = appConfig.getProperty(CONFIG_HTTP_HOST, "localhost")
  val httpPort = appConfig.getProperty(CONFIG_HTTP_PORT, "8080").toInt

  //TODO is this how child actors should be created - seems unsafe
  val gateway = system.actorOf(Props(new Gateway(appConfig)), name = "gateway")

  implicit val materializer = ActorMaterializer.create(context.system)

  import context.dispatcher

  val requestHandler: HttpRequest => Future[HttpResponse] = { httpRequest =>
    implicit val timeout = Timeout(120 seconds)

    val result = for (result <- gateway ? httpRequest) yield result match {
      case response: HttpResponse => response
      case any =>
        log.error("Gateway gave invalid response, expecting HttpResponse, got " + any)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> That's embarrassing..</h1>"))
    }

    result recover {
      case e: Throwable =>
        log.error("Gateway leaked exception", e)
        HttpResponse(status = StatusCodes.InternalServerError,
          entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "<h1> That's embarrassing..</h1>"))
    }
  }

  val bindingFuture = Http().bindAndHandleAsync(requestHandler, httoHost, httpPort)
  //TODO server cannot bind to the port

//  bindingFuture.onFailure {
//    case e: Exception => log.error("Failed to start http interface", e)
//  }

  try {
    val binding: ServerBinding = Await.result(bindingFuture, 10 seconds)
    println(binding)
  } catch {
    case e: Throwable =>
      println("re-throwing throwable caught during binding")
      throw e
  }


  override def preStart(): Unit = {
    log.info(s"Akka Http Server online at http://$httoHost:$httpPort/\nPress ^C to stop...")
  }

  override def postStop(): Unit = {
    log.info("unbinding server port " + httpPort)
//    Await.result(binding.unbind(), 15 seconds)
    log.info("server unbound, closing coordinator")
    super.postStop()
  }

  override def receive: Receive = {
    case any =>
  }
}

