package io.amient.akkahttp

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

  def fromConfig(appConfig: Properties)(implicit system: ActorSystem): Option[HttpInterface] = {
    if (!appConfig.containsKey(CONFIG_HTTP_PORT)) None else {
      val httpHost = appConfig.getProperty(CONFIG_HTTP_HOST, "localhost")
      val httpPort = appConfig.getProperty(CONFIG_HTTP_PORT, "8080").toInt
      Some(new HttpInterface(httpHost, httpPort))
    }
  }

}

class HttpInterface(val httpHost: String, val httpPort: Int)(implicit system: ActorSystem) {

  implicit val materializer = ActorMaterializer.create(system)

  val log = Logging.getLogger(system, this)

  val incoming: Source[IncomingConnection, Future[ServerBinding]] = Http().bind(httpHost, httpPort)

  @volatile private var binding: ServerBinding = null

  def bind(gateway: ActorRef): Unit = {
    close()
    log.info("binding http interface")
    val bindingFuture: Future[Http.ServerBinding] =
      incoming.to(Sink.foreach { connection =>
        connection.handleWithAsyncHandler { req =>

          val responsePromise = Promise[HttpResponse]()

          gateway ! HttpExchange(req.method, req.uri, req.headers, req.entity, responsePromise)

          responsePromise.future
        }
      }).run()

    binding = Await.result(bindingFuture, 10 seconds)
  }

  log.info(s"Akka Http Server online at http://$httpHost:$httpPort/\nPress ^C to stop...")

  def close(): Unit = {
    if (binding != null) {
      log.info("unbinding http interface")
      Await.result(binding.unbind(), 15 seconds)
    }
  }

}

