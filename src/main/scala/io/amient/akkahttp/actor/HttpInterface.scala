package io.amient.akkahttp.actor

import akka.actor.{Actor, ActorRef}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model._
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.util.Timeout

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class HttpInterface(val host: String, val httpPort: Int, val gateway: ActorRef) extends Actor {

  val log = Logging.getLogger(context.system, this)

  implicit val system = context.system

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
  val bindingFuture = Http().bindAndHandleAsync(requestHandler, host, httpPort)

  bindingFuture.onFailure {
    //TODO server cannot bind to the port
    case e: Exception => log.error("Failed to start http interface", e)
  }

  val binding: ServerBinding = Await.result(bindingFuture, 10 seconds)

  override def preStart(): Unit = {
    log.info(s"Akka Http Server online at http://$host:$httpPort/\nPress ^C to stop...")
  }

  override def postStop(): Unit = {
    log.info("unbinding server port " + httpPort)
    Await.result(binding.unbind(), 15 seconds)
    log.info("server unbound, closing coordinator")
    super.postStop()
  }

  override def receive: Receive = {
    case any =>
  }
}

