package io.amient.akkahttp.actor

import java.util.Properties

import akka.actor.{ActorRef, ActorSystem}
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.{IncomingConnection, ServerBinding}
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import io.amient.akkahttp.actor.Gateway.HttpExchange

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

object HttpInterface {
  final val CONFIG_HTTP_HOST = "http.host"
  final val CONFIG_HTTP_PORT = "http.port"
}

class HttpInterface(appConfig: Properties, gateway: ActorRef)(implicit system: ActorSystem) {

  implicit val materializer = ActorMaterializer.create(system)

  val log = Logging.getLogger(system, this)

  import HttpInterface._

  val httpHost = appConfig.getProperty(CONFIG_HTTP_HOST, "localhost")
  val httpPort = appConfig.getProperty(CONFIG_HTTP_PORT, "8080").toInt

  val incoming: Source[IncomingConnection, Future[ServerBinding]] = Http().bind(httpHost, httpPort)


  val bindingFuture: Future[Http.ServerBinding] =
    incoming.to(Sink.foreach { connection => // foreach materializes the source
      println("Accepted new connection from " + connection.remoteAddress)
      // ... and then actually handle the connection
      connection.handleWithAsyncHandler{ req =>

        val responsePromise = Promise[HttpResponse]()

        gateway ! HttpExchange(req.method, req.uri, req.headers, req.entity, responsePromise)

        responsePromise.future
      }
    }).run()


  val binding: ServerBinding = Await.result(bindingFuture, 10 seconds)

  log.info(s"Akka Http Server online at http://$httpHost:$httpPort/\nPress ^C to stop...")

  def close(): Unit = {
    log.info("unbinding server port " + httpPort)
    Await.result(binding.unbind(), 15 seconds)
  }

}

